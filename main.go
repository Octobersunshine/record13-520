package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"
	"time"

	"github.com/IBM/sarama"
)

type PartitionOffset struct {
	Partition     int32   `json:"partition"`
	CurrentOffset int64   `json:"current_offset"`
	LatestOffset  int64   `json:"latest_offset"`
	Lag           int64   `json:"lag"`
	LagRate       float64 `json:"lag_rate"`
	LagLevel      string  `json:"lag_level"`
	Metadata      string  `json:"metadata,omitempty"`
}

type ConsumerGroupOffset struct {
	GroupID    string            `json:"group_id"`
	Partitions []PartitionOffset `json:"partitions"`
}

type lagThresholds struct {
	AbsMinWarn int64
	AbsMinCrit int64
	PctWarn    float64
	PctCrit    float64
}

func parseThresholds(r *http.Request) lagThresholds {
	t := lagThresholds{
		AbsMinWarn: 100,
		AbsMinCrit: 1000,
		PctWarn:    5.0,
		PctCrit:    20.0,
	}
	if v := r.URL.Query().Get("abs_min_warn"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			t.AbsMinWarn = n
		}
	}
	if v := r.URL.Query().Get("abs_min_crit"); v != "" {
		if n, err := strconv.ParseInt(v, 10, 64); err == nil && n >= 0 {
			t.AbsMinCrit = n
		}
	}
	if v := r.URL.Query().Get("pct_warn"); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n >= 0 {
			t.PctWarn = n
		}
	}
	if v := r.URL.Query().Get("pct_crit"); v != "" {
		if n, err := strconv.ParseFloat(v, 64); err == nil && n >= 0 {
			t.PctCrit = n
		}
	}
	return t
}

func classifyLag(lag int64, latestOffset int64, t lagThresholds) (float64, string) {
	var lagRate float64
	if latestOffset > 0 {
		lagRate = float64(lag) / float64(latestOffset) * 100.0
	}
	if lag >= t.AbsMinCrit && lagRate >= t.PctCrit {
		return lagRate, "critical"
	}
	if lag >= t.AbsMinWarn && lagRate >= t.PctWarn {
		return lagRate, "warning"
	}
	return lagRate, "normal"
}

type ResetOffsetRequest struct {
	Topic      string  `json:"topic"`
	GroupID    string  `json:"group_id"`
	ResetTo    string  `json:"reset_to"`
	Timestamp  *int64  `json:"timestamp"`
	Datetime   string  `json:"datetime"`
	Partitions []int32 `json:"partitions"`
}

type ResetPartitionResult struct {
	Partition      int32  `json:"partition"`
	PreviousOffset int64  `json:"previous_offset"`
	NewOffset      int64  `json:"new_offset"`
	Error          string `json:"error,omitempty"`
}

type ResetOffsetResponse struct {
	Topic      string                `json:"topic"`
	GroupID    string                `json:"group_id"`
	ResetTo    string                `json:"reset_to"`
	Timestamp  int64                 `json:"timestamp,omitempty"`
	Partitions []ResetPartitionResult `json:"partitions"`
}

func resolveOffsetTimeMs(req ResetOffsetRequest) (int64, error) {
	switch req.ResetTo {
	case "earliest":
		return sarama.OffsetOldest, nil
	case "latest":
		return sarama.OffsetNewest, nil
	case "timestamp":
		if req.Timestamp != nil {
			return *req.Timestamp, nil
		}
		if req.Datetime != "" {
			t, err := time.Parse(time.RFC3339Nano, req.Datetime)
			if err != nil {
				t, err = time.Parse(time.RFC3339, req.Datetime)
				if err != nil {
					return 0, fmt.Errorf("invalid datetime format, use RFC3339 (e.g. 2024-06-21T10:30:00+08:00): %v", err)
				}
			}
			return t.UnixMilli(), nil
		}
		return 0, fmt.Errorf("timestamp or datetime is required when reset_to=timestamp")
	default:
		return 0, fmt.Errorf("invalid reset_to value: %q, must be one of: timestamp, earliest, latest", req.ResetTo)
	}
}

var (
	brokers string
	port    int
)

func init() {
	flag.StringVar(&brokers, "brokers", "localhost:9092", "Kafka brokers comma separated")
	flag.IntVar(&port, "port", 8080, "HTTP server port")
	flag.Parse()
}

func main() {
	http.HandleFunc("/offsets", getOffsetsHandler)
	http.HandleFunc("/reset-offsets", resetOffsetsHandler)
	addr := fmt.Sprintf(":%d", port)
	fmt.Printf("Server starting on %s...\n", addr)
	if err := http.ListenAndServe(addr, nil); err != nil {
		fmt.Printf("Error starting server: %v\n", err)
	}
}

func getOffsetsHandler(w http.ResponseWriter, r *http.Request) {
	topic := r.URL.Query().Get("topic")
	if topic == "" {
		http.Error(w, "topic parameter is required", http.StatusBadRequest)
		return
	}

	thresholds := parseThresholds(r)

	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0

	brokerList := strings.Split(brokers, ",")

	admin, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create Kafka admin client: %v", err), http.StatusInternalServerError)
		return
	}
	defer admin.Close()

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create Kafka client: %v", err), http.StatusInternalServerError)
		return
	}
	defer client.Close()

	partitions, err := client.Partitions(topic)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get partitions for topic %s: %v", topic, err), http.StatusInternalServerError)
		return
	}

	latestOffsets := make(map[int32]int64)
	for _, partition := range partitions {
		latest, err := client.GetOffset(topic, partition, sarama.OffsetNewest)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to get latest offset for partition %d: %v", partition, err), http.StatusInternalServerError)
			return
		}
		latestOffsets[partition] = latest
	}

	groups, err := admin.ListConsumerGroups()
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list consumer groups: %v", err), http.StatusInternalServerError)
		return
	}

	var response []ConsumerGroupOffset

	for groupID := range groups {
		offsetResp, err := admin.ListConsumerGroupOffsets(groupID, map[string][]int32{topic: nil})
		if err != nil {
			fmt.Printf("Error getting offsets for group %s: %v\n", groupID, err)
			continue
		}

		topicBlocks, ok := offsetResp.Blocks[topic]
		if !ok || len(topicBlocks) == 0 {
			continue
		}

		var groupPartitions []PartitionOffset
		for partition, block := range topicBlocks {
			latest, ok := latestOffsets[partition]
			if !ok {
				latest = 0
			}
			lag := latest - block.Offset
			if lag < 0 {
				lag = 0
			}
			lagRate, lagLevel := classifyLag(lag, latest, thresholds)
			groupPartitions = append(groupPartitions, PartitionOffset{
				Partition:     partition,
				CurrentOffset: block.Offset,
				LatestOffset:  latest,
				Lag:           lag,
				LagRate:       lagRate,
				LagLevel:      lagLevel,
				Metadata:      block.Metadata,
			})
		}

		if len(groupPartitions) > 0 {
			response = append(response, ConsumerGroupOffset{
				GroupID:    groupID,
				Partitions: groupPartitions,
			})
		}
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(response); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

func resetOffsetsHandler(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method not allowed, use POST", http.StatusMethodNotAllowed)
		return
	}

	var req ResetOffsetRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		http.Error(w, fmt.Sprintf("Invalid request body: %v", err), http.StatusBadRequest)
		return
	}

	if req.Topic == "" {
		http.Error(w, "topic is required", http.StatusBadRequest)
		return
	}
	if req.GroupID == "" {
		http.Error(w, "group_id is required", http.StatusBadRequest)
		return
	}
	if req.ResetTo == "" {
		req.ResetTo = "timestamp"
	}

	offsetTimeMs, err := resolveOffsetTimeMs(req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}

	config := sarama.NewConfig()
	config.Version = sarama.V3_0_0_0

	brokerList := strings.Split(brokers, ",")

	client, err := sarama.NewClient(brokerList, config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create Kafka client: %v", err), http.StatusInternalServerError)
		return
	}
	defer client.Close()

	admin, err := sarama.NewClusterAdmin(brokerList, config)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to create Kafka admin client: %v", err), http.StatusInternalServerError)
		return
	}
	defer admin.Close()

	allPartitions, err := client.Partitions(req.Topic)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to get partitions for topic %s: %v", req.Topic, err), http.StatusInternalServerError)
		return
	}

	targetPartitions := allPartitions
	if len(req.Partitions) > 0 {
		partitionSet := make(map[int32]bool, len(req.Partitions))
		for _, p := range req.Partitions {
			partitionSet[p] = true
		}
		var filtered []int32
		for _, p := range allPartitions {
			if partitionSet[p] {
				filtered = append(filtered, p)
			}
		}
		if len(filtered) == 0 {
			http.Error(w, "None of the specified partitions exist in the topic", http.StatusBadRequest)
			return
		}
		targetPartitions = filtered
	}

	previousOffsets, err := admin.ListConsumerGroupOffsets(req.GroupID, map[string][]int32{req.Topic: targetPartitions})
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to list current offsets for group %s: %v", req.GroupID, err), http.StatusInternalServerError)
		return
	}

	topicOffsetMap := make(map[int32]int64)
	for _, partition := range targetPartitions {
		offset, err := client.GetOffset(req.Topic, partition, offsetTimeMs)
		if err != nil {
			http.Error(w, fmt.Sprintf("Failed to resolve offset for partition %d at given time: %v", partition, err), http.StatusInternalServerError)
			return
		}
		topicOffsetMap[partition] = offset
	}

	offsets := map[string]map[int32]sarama.OffsetAndMetadata{
		req.Topic: {},
	}
	for partition, offset := range topicOffsetMap {
		offsets[req.Topic][partition] = sarama.OffsetAndMetadata{
			Offset:   offset,
			Metadata: "",
		}
	}

	commitResp, err := admin.AlterConsumerGroupOffsets(req.GroupID, offsets, nil)
	if err != nil {
		http.Error(w, fmt.Sprintf("Failed to alter consumer group offsets: %v", err), http.StatusInternalServerError)
		return
	}

	var results []ResetPartitionResult
	for _, partition := range targetPartitions {
		var prevOffset int64
		if block, ok := previousOffsets.Blocks[req.Topic][partition]; ok {
			prevOffset = block.Offset
		}

		result := ResetPartitionResult{
			Partition:      partition,
			PreviousOffset: prevOffset,
			NewOffset:      topicOffsetMap[partition],
		}

		if commitResp.Errors != nil {
			if topicErrors, ok := commitResp.Errors[req.Topic]; ok {
				if kerr, ok := topicErrors[partition]; ok && kerr != sarama.ErrNoError {
					result.Error = kerr.Error()
				}
			}
		}

		results = append(results, result)
	}

	resp := ResetOffsetResponse{
		Topic:      req.Topic,
		GroupID:    req.GroupID,
		ResetTo:    req.ResetTo,
		Timestamp:  offsetTimeMs,
		Partitions: results,
	}

	w.Header().Set("Content-Type", "application/json")
	if err := json.NewEncoder(w).Encode(resp); err != nil {
		http.Error(w, fmt.Sprintf("Failed to encode response: %v", err), http.StatusInternalServerError)
	}
}

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strconv"
	"strings"

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

package main

import (
	"encoding/json"
	"flag"
	"fmt"
	"net/http"
	"strings"

	"github.com/IBM/sarama"
)

type PartitionOffset struct {
	Partition     int32  `json:"partition"`
	CurrentOffset int64  `json:"current_offset"`
	LatestOffset  int64  `json:"latest_offset"`
	Lag           int64  `json:"lag"`
	Metadata      string `json:"metadata,omitempty"`
}

type ConsumerGroupOffset struct {
	GroupID    string            `json:"group_id"`
	Partitions []PartitionOffset `json:"partitions"`
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
			groupPartitions = append(groupPartitions, PartitionOffset{
				Partition:     partition,
				CurrentOffset: block.Offset,
				LatestOffset:  latest,
				Lag:           lag,
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

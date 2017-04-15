package main

import (
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

// Example SSE server in Golang.
//     $ go run sse.go

type Broker struct {
	// Events are pushed to this channel by the main events-gathering routine
	Notifier chan *sarama.ConsumerMessage
	// New client connections
	newClients chan KafkaUpdateChannel
	// Closed client connections
	closingClients chan KafkaUpdateChannel
	// Client connections registry
	clients map[KafkaUpdateChannel]bool
}

type KafkaUpdateChannel struct {
	KafkaCh chan *sarama.ConsumerMessage
	Topic   string
	Offset  int64
}

func NewServer() (broker *Broker) {
	// Instantiate a broker
	broker = &Broker{
		Notifier:       make(chan *sarama.ConsumerMessage, 1),
		newClients:     make(chan KafkaUpdateChannel),
		closingClients: make(chan KafkaUpdateChannel),
		clients:        make(map[KafkaUpdateChannel]bool),
	}
	// Set it running - listening and broadcasting events
	go broker.listen()

	return
}

func (broker *Broker) ServeHTTP(rw http.ResponseWriter, req *http.Request) {

	// Make sure that the writer supports flushing.
	flusher, ok := rw.(http.Flusher)

	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	topic := req.URL.Query().Get("topic")
	offsetStr := req.URL.Query().Get("offset")

	offset := sarama.OffsetOldest
	userSuppliedOffset, err := strconv.Atoi(offsetStr)

	if err == nil {
		offset = int64(userSuppliedOffset)
	}

	// Each connection registers its own message channel with the Broker's connections registry
	messageChan := make(chan *sarama.ConsumerMessage)

	// Signal the broker that we have a new connection
	KafkaChannel := KafkaUpdateChannel{
		KafkaCh: messageChan,
		Topic:   topic,
		Offset:  offset,
	}

	broker.newClients <- KafkaChannel

	// Remove this client from the map of connected clients
	// when this handler exits.
	defer func() {
		broker.closingClients <- KafkaChannel
	}()

	// Listen to connection close and un-register messageChan
	notify := rw.(http.CloseNotifier).CloseNotify()

	go func() {
		<-notify
		broker.closingClients <- KafkaChannel
	}()

	for {

		// Write to the ResponseWriter Server Sent Events compatible
		// Flush the data immediatly instead of buffering it for later.
		thing := <-messageChan
		json, err := json.Marshal(toCT(thing))
		if err == nil {
			fmt.Fprintf(rw, "data: %s\n\n", json)
			flusher.Flush()
		}
	}
}

type ConvertedThing struct {
	Key       string `json:"key"`
	Value     string `json:"value"`
	Offset    int64  `json:"offset"`
	Partition int32  `json:"partition"`
}

func toCT(c *sarama.ConsumerMessage) ConvertedThing {
	return ConvertedThing{
		Key:       string(c.Key[:]),
		Value:     string(c.Value[:]),
		Offset:    c.Offset,
		Partition: c.Partition,
	}
}

func (broker *Broker) listen() {
	for {
		select {
		case s := <-broker.newClients:

			// A new client has connected.
			// Register their message channel
			broker.clients[s] = true
			BackfillKafkaMessages(s)
			log.Printf("Client added. %d registered clients", len(broker.clients))
		case s := <-broker.closingClients:

			// A client has dettached and we want to
			// stop sending them messages.
			delete(broker.clients, s)
			log.Printf("Removed client. %d registered clients", len(broker.clients))
		case event := <-broker.Notifier:

			// We got a new event from the outside!
			// Send event to all connected clients
			for clientMessageChan := range broker.clients {
				clientMessageChan.KafkaCh <- event
			}
		}
	}

}

func GetENV(key, fallback string) string {
	value := os.Getenv(key)
	if len(value) == 0 {
		return fallback
	}
	return value
}

func BackfillKafkaMessages(s KafkaUpdateChannel) {
	ch := s.KafkaCh
	kafkaBrokerList := GetENV("KAFKA_BROKERS", "localhost:9092")
	topic := GetENV("TOPIC_NAME", s.Topic)
	bufferSize := 1

	initialOffset := s.Offset

	c, err := sarama.NewConsumer(strings.Split(kafkaBrokerList, ","), nil)

	if err != nil {
		log.Printf("Kafka connection error %s", err)
	}
	partitionList, err := c.Partitions(topic)
	log.Printf("Connected to %s , starting at %d", topic, initialOffset)

	if err != nil {
		log.Println("error getting partions %s", err)
	}
	// kafka
	var (
		messages = make(chan *sarama.ConsumerMessage, bufferSize)
		closing  = make(chan struct{})
		wg       sync.WaitGroup
	)

	for _, partition := range partitionList {
		pc, err := c.ConsumePartition(topic, partition, initialOffset)
		if err != nil {
			log.Println("Failed to start consumer for partition %d: %s", partition, err)
		}

		go func(pc sarama.PartitionConsumer) {
			<-closing
			pc.AsyncClose()
		}(pc)

		wg.Add(1)
		go func(pc sarama.PartitionConsumer) {
			defer wg.Done()
			for message := range pc.Messages() {
				messages <- message
			}
		}(pc)
	}
	go func() {
		for msg := range messages {
			ch <- msg
		}
	}()
}

func StreamTopicHandler(w http.ResponseWriter, r *http.Request) {
	broker := NewServer()
	broker.ServeHTTP(w, r)
}

func main() {
	log.Fatal("HTTP server error: ", http.ListenAndServe("0.0.0.0:4000", http.HandlerFunc(StreamTopicHandler)))
}

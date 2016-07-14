package main

import (
	"fmt"
	"log"
	"net/http"
	"strings"
	"sync"

	"github.com/Shopify/sarama"
)

// Example SSE server in Golang.
//     $ go run sse.go

type Broker struct {

	// Events are pushed to this channel by the main events-gathering routine
	Notifier chan []byte

	// New client connections
	newClients chan chan []byte

	// Closed client connections
	closingClients chan chan []byte

	// Client connections registry
	clients map[chan []byte]bool
}

func NewServer() (broker *Broker) {
	// Instantiate a broker
	broker = &Broker{
		Notifier:       make(chan []byte, 1),
		newClients:     make(chan chan []byte),
		closingClients: make(chan chan []byte),
		clients:        make(map[chan []byte]bool),
	}

	// Set it running - listening and broadcasting events
	go broker.listen()

	return
}

func (broker *Broker) ServeHTTP(rw http.ResponseWriter, req *http.Request) {

	// Make sure that the writer supports flushing.
	//
	flusher, ok := rw.(http.Flusher)

	if !ok {
		http.Error(rw, "Streaming unsupported!", http.StatusInternalServerError)
		return
	}

	rw.Header().Set("Content-Type", "text/event-stream")
	rw.Header().Set("Cache-Control", "no-cache")
	rw.Header().Set("Connection", "keep-alive")
	rw.Header().Set("Access-Control-Allow-Origin", "*")

	// Each connection registers its own message channel with the Broker's connections registry
	messageChan := make(chan []byte)

	// Signal the broker that we have a new connection
	broker.newClients <- messageChan

	// Remove this client from the map of connected clients
	// when this handler exits.
	defer func() {
		broker.closingClients <- messageChan
	}()

	// Listen to connection close and un-register messageChan
	notify := rw.(http.CloseNotifier).CloseNotify()

	go func() {
		<-notify
		broker.closingClients <- messageChan
	}()

	for {

		// Write to the ResponseWriter
		// Server Sent Events compatible
		fmt.Fprintf(rw, "data: %s\n\n", <-messageChan)

		// Flush the data immediatly instead of buffering it for later.
		flusher.Flush()
	}

}

func (broker *Broker) listen() {
	for {
		select {
		case s := <-broker.newClients:

			// A new client has connected.
			// Register their message channel
			broker.clients[s] = true
			sendKafkaMessages(s)
			log.Printf("Client added. %d registered clients", len(broker.clients))
		case s := <-broker.closingClients:

			// A client has dettached and we want to
			// stop sending them messages.
			delete(broker.clients, s)
			log.Printf("Removed client. %d registered clients", len(broker.clients))
		case event := <-broker.Notifier:

			// We got a new event from the outside!
			// Send event to all connected clients
			for clientMessageChan, _ := range broker.clients {
				clientMessageChan <- event
			}
		}
	}

}

func sendKafkaMessages(ch chan []byte) {
	kafkaBrokerList := "localhost:9092"
	topic := "example"
	bufferSize := 256

	initialOffset := sarama.OffsetOldest

	c, err := sarama.NewConsumer(strings.Split(kafkaBrokerList, ","), nil)

	if err != nil {
		log.Printf("Kafka connection error %s", err)
	}
	partitionList, err := c.Partitions(topic)

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
			log.Printf("Kafka Message: %s", msg.Value)

			ch <- []byte(msg.Value)
		}
	}()

}
func main() {
	broker := NewServer()
	log.Fatal("HTTP server error: ", http.ListenAndServe("localhost:3000", broker))
}

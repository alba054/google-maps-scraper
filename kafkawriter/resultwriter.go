package kafkawriter

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"log"
	"time"

	"github.com/gosom/google-maps-scraper/gmaps"
	"github.com/gosom/scrapemate"
	"github.com/segmentio/kafka-go"
)

func NewResultWriter(writer *kafka.Writer) scrapemate.ResultWriter {
	return &resultWriter{writer: writer}
}

type resultWriter struct {
	writer *kafka.Writer
}

func (r *resultWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	for result := range in {
		entry, ok := result.Data.(*gmaps.Entry)

		if !ok {
			return errors.New("invalid data type")
		}

		// defer writer.Close()

		// The message you want to send
		message, err := json.Marshal(entry)

		if err != nil {
			fmt.Println(err)
		}

		// Write a message to the Kafka topic
		err = r.writer.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", time.Now().Unix())), // Optional: Key for partitioning
			Value: message,
		})
		if err != nil {
			log.Fatalf("Failed to write message to Kafka: %s", err)
		}

		fmt.Println("Message successfully written to Kafka topic:", "sample-data-batch-01")

		// if err := r.saveEntry(ctx, entry); err != nil {
		// 	return err
		// }
	}

	return nil
}

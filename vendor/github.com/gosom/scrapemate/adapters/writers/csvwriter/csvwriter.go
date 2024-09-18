package csvwriter

import (
	"context"
	"encoding/csv"
	"encoding/json"
	"fmt"
	"log"
	"reflect"
	"sync"
	"time"

	"github.com/gosom/scrapemate"
	"github.com/segmentio/kafka-go"
)

var _ scrapemate.ResultWriter = (*csvWriter)(nil)

type csvWriter struct {
	w       *csv.Writer
	kafkawr *kafka.Writer
	once    sync.Once
}

// NewCsvWriter creates a new csv writer
func NewCsvWriter(w *csv.Writer) scrapemate.ResultWriter {
	// Define the Kafka broker and topic
	brokerAddress := []string{"kafka01.research.ai:9092", "kafka02.research.ai:9092", "kafka03.research.ai:9092"} // Change this to your Kafka broker address
	topic := "sample-data-batch-01"

	// Create a new Kafka writer (producer)
	writer := kafka.NewWriter(kafka.WriterConfig{
		Brokers:  brokerAddress,
		Topic:    topic,
		Balancer: &kafka.LeastBytes{}, // Load-balances the messages
	})

	return &csvWriter{w: w, kafkawr: writer}
}

// Run runs the writer.
func (c *csvWriter) Run(ctx context.Context, in <-chan scrapemate.Result) error {
	// defer c.kafkawr.Close()
	for result := range in {

		// The message you want to send
		message, err := json.Marshal(result.Data)

		if err != nil {
			fmt.Println(err)
		}

		// Write a message to the Kafka topic
		err = c.kafkawr.WriteMessages(ctx, kafka.Message{
			Key:   []byte(fmt.Sprintf("Key-%d", time.Now().Unix())), // Optional: Key for partitioning
			Value: message,
		})
		if err != nil {
			log.Fatalf("Failed to write message to Kafka: %s", err)
		}

		fmt.Println("Message successfully written to Kafka topic:", "sample-data-batch-01")

		// elements, err := c.getCsvCapable(result.Data)
		// if err != nil {
		// 	return err
		// }

		// if len(elements) == 0 {
		// 	continue
		// }

		// c.once.Do(func() {
		// 	// I don't like this, but I don't know how to do it better
		// 	_ = c.w.Write(elements[0].CsvHeaders())
		// })

		// for _, element := range elements {
		// 	if err := c.w.Write(element.CsvRow()); err != nil {
		// 		return err
		// 	}
		// }

		// c.w.Flush()
	}

	return c.w.Error()
}

func (c *csvWriter) getCsvCapable(data any) ([]scrapemate.CsvCapable, error) {
	var elements []scrapemate.CsvCapable

	if interfaceIsSlice(data) {
		s := reflect.ValueOf(data)

		for i := 0; i < s.Len(); i++ {
			val := s.Index(i).Interface()
			if element, ok := val.(scrapemate.CsvCapable); ok {
				elements = append(elements, element)
			} else {
				return nil, fmt.Errorf("%w: unexpected data type: %T", scrapemate.ErrorNotCsvCapable, val)
			}
		}
	} else if element, ok := data.(scrapemate.CsvCapable); ok {
		elements = append(elements, element)
	} else {
		return nil, fmt.Errorf("%w: unexpected data type: %T", scrapemate.ErrorNotCsvCapable, data)
	}

	return elements, nil
}

func interfaceIsSlice(t any) bool {
	//nolint:exhaustive // we only need to check for slices
	switch reflect.TypeOf(t).Kind() {
	case reflect.Slice:
		return true
	default:
		return false
	}
}

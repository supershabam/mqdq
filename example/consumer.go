package main

import (
	"log"

	"github.com/supershabam/mqhammer"
)

func main() {

	c := mqhammer.RabbitConsumer{
		Config: mqhammer.RabbitConsumerConfig{
			URI:         "amqp://localhost",
			Exchange:    "a",
			Queue:       "a",
			Key:         "a",
			ConsumerTag: "1",
		},
	}

	done := make(chan struct{})
	deliveries, err := c.Consume(done)
	if err != nil {
		log.Fatal(err)
	}
	for d := range deliveries {
		log.Printf("%+v", d)
	}
}
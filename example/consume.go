package main

import (
	"log"
	"time"

	"github.com/supershabam/mqhammer"
)

func main() {
	mq1, err := mqhammer.NewRabbitConsumer(mqhammer.RabbitConsumerConfig{
		URI:          "amqp://dggjvxhj:QwKHxFeKPxRvpQ_HwRVOYzFfE1-lsy7h@tiger.cloudamqp.com/dggjvxhj",
		Exchange:     "a",
		ExchangeType: "direct",
		Queue:        "a",
		Key:          "a",
		ConsumerTag:  "1",
	})
	if err != nil {
		log.Fatal(err)
	}
	mq2, err := mqhammer.NewRabbitConsumer(mqhammer.RabbitConsumerConfig{
		URI:          "amqp://rfvrpejq:nqWGhu9KaPPqwgdqVpfr0RtDVTGPqHuD@tiger.cloudamqp.com/rfvrpejq",
		Exchange:     "a",
		ExchangeType: "direct",
		Queue:        "a",
		Key:          "a",
		ConsumerTag:  "1",
	})
	if err != nil {
		log.Fatal(err)
	}
	merged := mqhammer.MergeConsumer{
		Consumers: []mqhammer.Consumer{
			mq1,
			mq2,
		},
	}
	deliveries := merged.Consume()
	for d := range deliveries {
		log.Printf("%+v", d)
		log.Print("processing....")
		// simulate processing delay
		// if you kill the program inbetween logging the message and
		// acking the message, it will be requeued for another consumer
		time.Sleep(5 * time.Second)
		d.Ack()
		log.Print("acked")
	}
	if err = merged.Err(); err != nil {
		log.Fatal(err)
	}
}

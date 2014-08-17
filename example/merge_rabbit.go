package main

import (
	"log"

	"github.com/supershabam/mqdq/mqdq"
	"github.com/supershabam/mqdq/rabbit"
)

func main() {
	r1, err := rabbit.NewConsumer("amqp://localhost?exchange=r1&queue=r1&bind_key=r1")
	if err != nil {
		log.Fatal(err)
	}
	r2, err := rabbit.NewConsumer("amqp://localhost?exchange=r2&queue=r2&bind_key=r2")
	if err != nil {
		log.Fatal(err)
	}
	c := mqdq.Merger{
		Consumers: []mqdq.Consumer{r1, r2},
	}
	for d := range c.Consume() {
		log.Printf("%s", d.Msg)
		d.Ack()
	}
	if err := c.Err(); err != nil {
		log.Fatal(err)
	}
}

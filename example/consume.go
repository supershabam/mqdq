package main

import (
	"log"
	"time"

	"github.com/supershabam/mqhammer"
	"github.com/supershabam/mqhammer/impl"
)

func main() {
	rabbitFn := func() (mqhammer.Consumer, error) {
		return impl.NewRabbitURL("amqp://dggjvxhj:QwKHxFeKPxRvpQ_HwRVOYzFfE1-lsy7h@tiger.cloudamqp.com/dggjvxhj?exchange=1&exchange_type=direct&queue=a&key=a")
	}
	recon := mqhammer.NewReconnector(rabbitFn, time.Second)
	// mq2, err := impl.NewRabbitURL("amqp://rfvrpejq:nqWGhu9KaPPqwgdqVpfr0RtDVTGPqHuD@tiger.cloudamqp.com/rfvrpejq?exchange=1&exchange_type=direct&queue=a&key=a")
	// if err != nil {
	// 	log.Fatal(err)
	// }
	merged := mqhammer.NewMerger(recon)
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
	if err := merged.Err(); err != nil {
		log.Fatal(err)
	}
}

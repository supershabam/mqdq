mqdq
====

_Message Queue Dequeue_

This is an interface for consuming from a message queue that gives you some composibility.

Mix and match consuming from multiple message queue backends or configurations.

### Example

```go
package main

// The Consumer interface let's us deal with message queues in an abstract way. We can
// use the Merge consumer to combine multiple Consumers and forget about the details.

import (
  "log"

  "github.com/supershabam/mqdq/mqdq"
  "github.com/supershabam/mqdq/rabbit"
)

func main() {
  // set up the first rabbit consumer. This consumer reads durably, so that a message is
  // to a different consumer if we don't ack it.
  r1, err := rabbit.NewConsumer("amqp://localhost?exchange=r1&queue=r1&bind_key=r1")
  if err != nil {
    log.Fatal(err)
  }
  // our second rabbit consumer is not durable, if we get a message delivered to us, the
  // message queue consideres it fulfilled and won't redeliver (even if we crash before acking)
  r2, err := rabbit.NewConsumer("amqp://localhost?exchange=r2&queue=r2&bind_key=r2&durable=false")
  if err != nil {
    log.Fatal(err)
  }
  // the Merger allows us to pass in many consumers which will be read in parallel. You could be reading
  // from rabbit, sqs, zmq, http... anything that implements the Consumer interface.
  consumer := mqdq.Merger{
    Consumers: []mqdq.Consumer{r1, r2},
  }
  for delivery := range consumer.Consume() {
    // A delivery consists of a Msg []byte payload, and a way to Acknowledge the message as being processed
    log.Printf("%s", delivery.Msg)

    // even though not all consumers are durable, you MUST Ack the delivery after you are done processing the
    // message. For non-durable consumers, this is just a No-op.
    // If you would like to NOT handle a message, but have it be requeued to a different consumer, you may call
    // Nack on the delivery (if your consumer supports nacking).
    delivery.Ack()
  }
  // After the consume channel closes, you must check if the consumption ended because of an error, or
  // because of a normal shutdown.
  if err := consumer.Err(); err != nil {
    log.Fatal(err)
  }
}
```

### LICENSE 

MIT
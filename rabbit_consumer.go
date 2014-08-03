package mqhammer

import (
	"github.com/streadway/amqp"
)

type RabbitConsumerConfig struct {
	URI          string
	Exchange     string
	ExchangeType string
	Queue        string
	Key          string
	ConsumerTag  string
}

type RabbitConsumer struct {
	Config RabbitConsumerConfig
	err    error
}

func (c RabbitConsumer) Err() error {
	return c.err
}

type rabbitAcknowledger struct {
	d amqp.Delivery
}

func (a rabbitAcknowledger) Ack() {
	a.d.Ack(false)
}

func (a rabbitAcknowledger) Nack() {
	a.d.Nack(false, true)
}

func (c *RabbitConsumer) Consume(done <-chan struct{}) (<-chan Delivery, error) {
	conn, err := amqp.Dial(c.Config.URI)
	if err != nil {
		return nil, err
	}
	channel, err := conn.Channel()
	if err != nil {
		return nil, err
	}
	if err := channel.ExchangeDeclare(
		c.Config.Exchange,     // name of the exchange
		c.Config.ExchangeType, // type
		true,  // durable
		false, // delete when complete
		false, // internal
		false, // noWait
		nil,   // arguments
	); err != nil {
		return nil, err
	}
	_, err = channel.QueueDeclare(
		c.Config.Queue, // name of the queue
		true,           // durable
		false,          // delete when usused
		false,          // exclusive
		false,          // noWait
		nil,            // arguments
	)
	if err != nil {
		return nil, err
	}
	if err = channel.QueueBind(
		c.Config.Queue,    // name of the queue
		c.Config.Key,      // bindingKey
		c.Config.Exchange, // sourceExchange
		false,             // noWait
		nil,               // arguments
	); err != nil {
		return nil, err
	}
	in, err := channel.Consume(
		c.Config.Queue,       // name
		c.Config.ConsumerTag, // consumerTag,
		false,                // noAck
		false,                // exclusive
		false,                // noLocal
		false,                // noWait
		nil,                  // arguments
	)
	if err != nil {
		return nil, err
	}
	out := make(chan Delivery)
	closed := false
	go func() {
		defer close(out)

		for {
			select {
			case <-done:
				closed = true
				// this will cause "in" to close
				conn.Close()
			case d, ok := <-in:
				if !ok {
					return
				}
				if closed {
					// make sure mq knows we are not fulfilling the message
					// multiple=false, requeue=true
					d.Nack(false, true)
					continue
				}
				out <- Delivery{
					Ackr: rabbitAcknowledger{d},
					Msg:  d.Body,
				}
			}
		}
	}()
	return out, nil
}

package mqdq

type Acknowledger interface {
	Ack()
	Nack()
}

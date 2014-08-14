package mqdq

type Delivery struct {
	Ackr Acknowledger
	Msg  []byte
}

func (d Delivery) Ack() {
	d.Ackr.Ack()
}

func (d Delivery) Nack() {
	d.Ackr.Nack()
}

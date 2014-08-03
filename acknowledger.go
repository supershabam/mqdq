package mqhammer

type Acknowledger interface {
	Ack()
	Nack()
}

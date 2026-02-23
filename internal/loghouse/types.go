package loghouse

type Event struct {
	Raw []byte
}

type Batch struct {
	Events []Event
	Bytes  int
}

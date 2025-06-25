package transport

type Envelope struct {
    Type string
    From int
    Seq  uint64
    Data []byte
}

type Transport interface {
    Send(to int, msg *Envelope) error
    Broadcast(msg *Envelope) error
    Start() error
    Close() error
    Addr() string
}

package transport

// Envelope es la envoltura de todos los mensajes enviados por RPC o in-memory.
type Envelope struct {
    Type string
    From int
    Seq  uint64
    Data []byte
}

type Transport interface {
    Start() error
    Send(to int, msg *Envelope) error
    Broadcast(msg *Envelope) error
    Close() error
    Addr() string
}
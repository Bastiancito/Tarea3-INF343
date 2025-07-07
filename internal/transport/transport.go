package transport

const (
    EnvelopeTypePing           = "Ping"
    EnvelopeTypePingResponse   = "PingResponse"
    EnvelopeTypeElection       = "Election"
    EnvelopeTypeCoordinator    = "Coordinator"
    EnvelopeTypeRequestState   = "RequestState"
    EnvelopeTypeStateResponse  = "StateResponse"
    EnvelopeTypeReplicate      = "Replicate"
    EnvelopeTypeSubmitEvent    = "SubmitEvent"    
)

type Envelope struct {
    Type string
    From int
    Seq  uint64
    Data []byte
}


type Transport interface {
    Start() error
    Send(peerID int, msg *Envelope) error
    Broadcast(msg *Envelope) error
    Close() error        
    Addr() string
    Receive() <-chan *Envelope    
}
package transport

import (
    "fmt"
    "log"
    "net"
    "net/rpc"
    "github.com/google/uuid"
)

type EventRecord struct {
    ID    uuid.UUID `json:"id"`
    Value string    `json:"value"`
}

type rpcTransport struct {
    id     int
    addr   string
    peers  map[int]string
    server *rpc.Server
    ln     net.Listener

    msgChan chan *Envelope
}

type rpcAPI struct{ parent *rpcTransport }

func NewRPC(id int, addr string, peers map[int]string) Transport {
    return &rpcTransport{
    id:      id,
    addr:    addr,
    peers:   peers,
    msgChan: make(chan *Envelope, 64),
}
}

func (t *rpcTransport) Start() error {
    t.server = rpc.NewServer()
    if err := t.server.RegisterName("Node", &rpcAPI{parent: t}); err != nil {
        return err
    }
    ln, err := net.Listen("tcp", t.addr)
    if err != nil {
        return err
    }
    t.ln = ln
    go t.server.Accept(ln)
    log.Printf("[T%d] RPC listening on %s", t.id, t.addr)
    return nil
}

func (t *rpcTransport) Close() error { return t.ln.Close() }
func (t *rpcTransport) Addr() string { return t.addr }

func (t *rpcTransport) Send(peerID int, msg *Envelope) error {
    client, ok := t.clients[peerID]
    if !ok {
        addr := t.peers[peerID]
        c, err := rpc.Dial("tcp", addr)
        if err != nil {
            return err
        }
        t.clients[peerID] = c
        client = c
    }
    var ack bool
    return client.Call("Node.Handle", msg, &ack)
}

func (t *rpcTransport) Broadcast(msg *Envelope) error {
    for peerID := range t.peers {
        if peerID == t.id {
            continue
        }
        _ = t.Send(peerID, msg) 
    }
    return nil
}


func (api *rpcAPI) Handle(msg *Envelope, ack *bool) error {
    api.parent.msgChan <- msg
    *ack = true
    return nil
}


func (t *rpcTransport) Receive() <-chan *Envelope {
    return t.msgChan
}

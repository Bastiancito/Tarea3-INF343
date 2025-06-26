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
    return &rpcTransport{id: id, addr: addr, peers: peers}
}

func (t *rpcTransport) Start() error {
    t.server = rpc.NewServer()
    if err := t.server.RegisterName("Msg", &rpcAPI{parent: t}); err != nil {
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

func (t *rpcTransport) Send(to int, msg *Envelope) error {
    peerAddr, ok := t.peers[to]
    if !ok {
        return fmt.Errorf("unknown peer %d", to)
    }
    client, err := rpc.Dial("tcp", peerAddr)
    if err != nil {
        return err
    }
    defer client.Close()
    var ack bool
    return client.Call("Msg.Handle", msg, &ack)
}

func (t *rpcTransport) Broadcast(msg *Envelope) error {
    for id := range t.peers {
        if id == t.id { continue }
        if err := t.Send(id, msg); err != nil {
            log.Printf("broadcast to %d failed: %v", id, err)
        }
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

package transport

import (
    "fmt"
    "net"
    "net/rpc"
)

type rpcTransport struct {
    id      int
    addr    string
    peers   map[int]string
    server  *rpc.Server
    ln      net.Listener
    msgChan chan *Envelope
}

type rpcAPI struct {
    parent *rpcTransport
}

func NewRPC(id int, addr string, peers map[int]string) Transport {
    t := &rpcTransport{
        id:      id,
        addr:    addr,
        peers:   peers,
        msgChan: make(chan *Envelope, 64),
    }
    t.server = rpc.NewServer()
    t.server.RegisterName("Node", &rpcAPI{parent: t})
    return t
}

func (t *rpcTransport) Start() error {
    ln, err := net.Listen("tcp", t.addr)
    if err != nil {
        return err
    }
    t.ln = ln
    go t.server.Accept(ln)
    return nil
}

func (t *rpcTransport) Send(to int, msg *Envelope) error {
    addr, ok := t.peers[to]
    if !ok {
        return fmt.Errorf("unknown peer %d", to)
    }
    client, err := rpc.Dial("tcp", addr)
    if err != nil {
        return err
    }
    defer client.Close()
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

func (t *rpcTransport) Receive() <-chan *Envelope {
    return t.msgChan
}

func (t *rpcTransport) Close() error {
    if t.ln != nil {
        return t.ln.Close()
    }
    return nil
}

func (t *rpcTransport) Addr() string {
    return t.addr
}

func (api *rpcAPI) Handle(msg *Envelope, ack *bool) error {
    api.parent.msgChan <- msg
    *ack = true
    return nil
}

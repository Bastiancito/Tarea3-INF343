package transport

import (
    "net"
    "net/rpc"
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
    Close()
}

type rpcTransport struct {
    id      int
    addr    string
    peers   map[int]string
    clients map[int]*rpc.Client
    server  *rpc.Server
    msgChan chan *Envelope
}

func NewRPC(id int, addr string, peers map[int]string) Transport {
    t := &rpcTransport{
        id:      id,
        addr:    addr,
        peers:   peers,
        clients: make(map[int]*rpc.Client),
        server:  rpc.NewServer(),
        msgChan: make(chan *Envelope, 64),
    }
    t.server.RegisterName("Node", &rpcAPI{parent: t})
    return t
}

func (t *rpcTransport) Start() error {
    ln, err := net.Listen("tcp", t.addr)
    if err != nil {
        return err
    }
    go t.server.Accept(ln)
    return nil
}

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

func (t *rpcTransport) Close() {
    for _, c := range t.clients {
        c.Close()
    }
}

func (t *rpcTransport) Receive() <-chan *Envelope {
    return t.msgChan
}

type rpcAPI struct {
    parent *rpcTransport
}

func (r *rpcAPI) Handle(req *Envelope, resp *bool) error {
    r.parent.msgChan <- req
    *resp = true
    return nil
}
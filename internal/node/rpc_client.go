package node

import (
  "fmt"
  "net/rpc"
  "github.com/Bastiancito/tarea3/internal/transport"
)

type RPCClient struct {
    selfID int
    peers  map[int]string
}

func NewRPCClient(selfID int, peers map[int]string) *RPCClient {
    return &RPCClient{selfID: selfID, peers: peers}
}

func (c *RPCClient) send(to int, env *transport.Envelope) error {
    addr, ok := c.peers[to]
    if !ok {
        return fmt.Errorf("peer %d desconocido", to)
    }
    client, err := rpc.Dial("tcp", addr)
    if err != nil {
        return err
    }
    defer client.Close()

    var ack bool
    return client.Call("Node.Handle", env, &ack)
}

func (c *RPCClient) Ping(to int) (bool, error) {
    env := &transport.Envelope{
        Type: "Ping",
        From: c.selfID,
    }
    err := c.send(to, env)
    return err == nil, err
}

func (c *RPCClient) Election(to int) (bool, error) {
    env := &transport.Envelope{
        Type: "Election",
        From: c.selfID,
    }
    err := c.send(to, env)
    return err == nil, err
}

func (c *RPCClient) RequestState(to int, lastSeq uint64) error {
    env := &transport.Envelope{
        Type: "RequestState",
        From: c.selfID,
        Seq:  lastSeq,
    }
    return c.send(to, env)
 }
 
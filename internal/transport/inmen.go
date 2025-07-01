package transport

import (
    "fmt"
    "sync"
)

var (
    registry = make(map[int]chan *Envelope)
    regMu    sync.RWMutex
)

type inMemory struct {
    id    int
    inbox chan *Envelope
}

func NewInMemory(id int) Transport {
    regMu.Lock()
    defer regMu.Unlock()
    ch := make(chan *Envelope, 64)
    registry[id] = ch
    return &inMemory{id: id, inbox: ch}
}

func (t *inMemory) Start() error {
    return nil
}

func (t *inMemory) Send(to int, msg *Envelope) error {
    regMu.RLock()
    dst, ok := registry[to]
    regMu.RUnlock()
    if !ok {
        return fmt.Errorf("peer %d not in registry", to)
    }
    dst <- msg
    return nil
}

func (t *inMemory) Broadcast(msg *Envelope) error {
    regMu.RLock()
    defer regMu.RUnlock()
    for pid, ch := range registry {
        if pid == t.id {
            continue
        }
        ch <- msg
    }
    return nil
}

func (t *inMemory) Receive() <-chan *Envelope {
    return t.inbox
}

func (t *inMemory) Close() error {
    close(t.inbox)
    return nil
}

func (t *inMemory) Addr() string {
    return "inmem"
}

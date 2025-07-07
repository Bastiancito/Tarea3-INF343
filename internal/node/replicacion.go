package node

import (
    "encoding/json"
    "errors"
    "fmt"
    "github.com/google/uuid"
    "github.com/Bastiancito/tarea3/internal/transport"
)

func (n *Node) ProcessEvent(value string) (uint64, error) {
    if !n.isLeader {
        return 0, errors.New("not the leader")
    }

    ev := EventRecord{ID: uuid.New(), Value: value}
    data := n.eventToBytes(ev)

    n.mu.RLock()
    nextSeq := n.state.Sequence + 1
    n.mu.RUnlock()
    n.log("Asignando sequencia %d al evento: %s", nextSeq, ev.Value)

    msg := &transport.Envelope{
        Type: transport.EnvelopeTypeReplicate,
        From: n.cfg.SelfID,
        Seq:  nextSeq,
        Data: data,
    }

    if err := n.transport.Broadcast(msg); err != nil {
        return 0, fmt.Errorf("failed to broadcast event: %v", err)
    }
    if err := n.transport.Broadcast(msg); err!=nil{
        n.log("Advertencia: no se pudo replicar el evento a todos los nodos: %v", err)
    }

    n.log("Evento replicado a nodos: %s (seq=%d)", ev.Value, nextSeq)
    n.mu.Lock()
    defer n.mu.Unlock()

    n.state.Sequence = nextSeq
    n.state.Log = append(n.state.Log, ev)
    if err := n.state.Save(n.cfg.StateFile); err != nil {
        return 0, fmt.Errorf("failed to save state: %v", err)
    }

    return nextSeq, nil
}

func (n *Node) eventToBytes(event EventRecord) []byte {
    data, _ := json.Marshal(event)
    return data
}


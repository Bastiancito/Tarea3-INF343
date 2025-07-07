package node

import (
    "encoding/json"
    "errors"
    "github.com/google/uuid"
    "github.com/Bastiancito/tarea3/internal/transport"
)

func (n *Node) ProcessEvent(value string) (uint64, error) {
    n.mu.Lock()
    defer n.mu.Unlock()

    if !n.isLeader {
        return 0, errors.New("not the leader")
    }

    n.state.Sequence++
    ev := EventRecord{ID: uuid.New(), Value: value}
    n.state.Log = append(n.state.Log, ev)
    if err := n.state.Save(n.cfg.StateFile); err != nil {
        return 0, fmt.Errorf("failed to save state: %v", err)
    }

    msg := &transport.Envelope{
        Type: transport.EnvelopeTypeReplicate,
        From: n.cfg.SelfID,
        Seq:  n.state.Sequence,
        Data: n.eventToBytes(ev),
    }
    if err := n.transport.Broadcast(msg); err != nil {
        return 0, fmt.Errorf("failed to broadcast event: %v", err)
    }
    return n.state.Sequence, nil    
}

func (n *Node) eventToBytes(event EventRecord) []byte {
    data, _ := json.Marshal(event)
    return data
}


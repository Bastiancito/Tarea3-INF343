package node

import (
    "time"
    "github.com/Bastiancito/tarea3/internal/transport"
)

func (n *Node) monitorLeader() {
    maxID := 0
    for pid := range n.cfg.Peers {
        if pid > maxID {
            maxID = pid
        }
    }

    initialDelay := time.Duration(maxID-n.cfg.SelfID) *
        time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond
    time.Sleep(initialDelay)

    heartbeatTicker := time.NewTicker(time.Duration(n.cfg.HeartbeatMs) * time.Millisecond)
    electionTimer := time.NewTimer(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)
    defer heartbeatTicker.Stop()
    defer electionTimer.Stop()

    for {
        select {
        case <-heartbeatTicker.C:
            if n.isLeader {
                n.transport.Broadcast(&transport.Envelope{
                    Type: "Heartbeat",
                    From: n.cfg.SelfID,
                })
            }
        case <-n.heartbeatCh:
            electionTimer.Reset(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)
        case <-electionTimer.C:
            n.electionCh <- struct{}{}
        case <-n.ctx.Done():
            return
        }
    }
}

func (n *Node) sendHeartbeats() {
    ticker := time.NewTicker(time.Duration(n.cfg.HeartbeatMs) * time.Millisecond)
    defer ticker.Stop()

    n.log("Iniciando envío de heartbeats como líder")
    
    for {
        select {
        case <-ticker.C:
            msg := &transport.Envelope{
                Type: "Heartbeat",
                From: n.cfg.SelfID,
                Seq:  n.state.Sequence,
            }
            n.transport.Broadcast(msg)
        case <-n.ctx.Done():
            return
        }
    }
}
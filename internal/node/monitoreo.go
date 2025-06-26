package node

import (
    "time"
    "github.com/Bastiancito/tarea3/internal/transport"
)

func (n *Node) monitorLeader() {
    heartbeatTimeout := time.Duration(n.cfg.HeartbeatMs * 3) * time.Millisecond
    timer := time.NewTimer(heartbeatTimeout)
    defer timer.Stop()

    for {
        select {
        case <-n.heartbeatCh:
            if !timer.Stop() {
                <-timer.C
            }
            timer.Reset(heartbeatTimeout)
        case <-timer.C:
            if !n.isLeader {
                n.electionCh <- struct{}{}
            }
            timer.Reset(heartbeatTimeout)
        case <-n.ctx.Done():
            return
        }
    }
}

func (n *Node) sendHeartbeats() {
    ticker := time.NewTicker(time.Duration(n.cfg.HeartbeatMs) * time.Millisecond)
    defer ticker.Stop()

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
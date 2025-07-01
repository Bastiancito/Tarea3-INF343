package node

import (
    "time"

    "github.com/Bastiancito/tarea3/internal/transport"
)

func (n *Node) runLeaderElection() {
    for {
        select {
        case <-n.electionCh:
            n.log("Se recibió señal de elección")
            n.startElection()
        case <-n.ctx.Done():
            return
        }
    }
}

func (n *Node) startElection() {
    n.mu.Lock()
    n.leaderID = -1
    n.isLeader = false
    n.mu.Unlock()

    higherExists := false
    for peerID := range n.cfg.Peers {
        if peerID > n.cfg.SelfID {
            higherExists = true
            msg := &transport.Envelope{
                Type: "Election",
                From: n.cfg.SelfID,
            }
            _ = n.transport.Send(peerID, msg)
        }
    }

    if !higherExists {
        n.becomeLeader()
        return
    }

    timeout := time.NewTimer(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)
    defer timeout.Stop()

    for {
        select {
        case <-timeout.C:
            n.becomeLeader()
            return
        case <-n.electionCh:
            n.log("Se recibió nueva señal de elección")
            n.startElection()
            return
        case <-n.ctx.Done():
            return
        }
    }
}

func (n *Node) becomeLeader() {
    n.mu.Lock()
    n.isLeader = true
    n.leaderID = n.cfg.SelfID
    n.mu.Unlock()

    n.log("¡Elegido como nuevo líder!")
    announcement := &transport.Envelope{
        Type: "LeaderAnnouncement",
        From: n.cfg.SelfID,
    }
    n.transport.Broadcast(announcement)
}

package node

import (
    "time"
    "github.com/Bastiancito/tarea3/internal/transport"
)

func (n *Node) runLeaderElection() {
    electionTimer := time.NewTimer(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)
    defer electionTimer.Stop()

    for {
        select {
        case <-electionTimer.C:
            if !n.isLeader {
                n.startElection()
            }
            electionTimer.Reset(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)
        case <-n.electionCh:
            if !electionTimer.Stop() {
                <-electionTimer.C
            }
            electionTimer.Reset(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)
        case <-n.ctx.Done():
            return
        }
    }
}

func (n *Node) startElection() {
    n.mu.Lock()
    defer n.mu.Unlock()

    higherNodes := n.getHigherNodes()
    if len(higherNodes) == 0 {
        n.becomeLeader()
        return
    }

    msg := &transport.Envelope{
        Type: "Election",
        From: n.cfg.SelfID,
    }

    responses := make(chan bool, len(higherNodes))
    for _, id := range higherNodes {
        go func(peerID int) {
            err := n.transport.Send(peerID, msg)
            responses <- (err == nil)
        }(id)
    }

    timeout := time.After(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)
    receivedResponse := false

    for i := 0; i < len(higherNodes); i++ {
        select {
        case ok := <-responses:
            if ok {
                receivedResponse = true
            }
        case <-timeout:
            break
        }
    }

    if !receivedResponse {
        n.becomeLeader()
    }
}

func (n *Node) becomeLeader() {
    n.isLeader = true
    n.leaderID = n.cfg.SelfID

    msg := &transport.Envelope{
        Type: "LeaderAnnouncement",
        From: n.cfg.SelfID,
    }
    n.transport.Broadcast(msg)

    go n.sendHeartbeats()
}

func (n *Node) getHigherNodes() []int {
    var higher []int
    for id := range n.cfg.Peers {
        if id > n.cfg.SelfID {
            higher = append(higher, id)
        }
    }
    return higher
}
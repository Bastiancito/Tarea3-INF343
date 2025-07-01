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
        case <-n.electionCh:
            n.log("Se recibió señal de elección")
            n.startElection()
            electionTimer.Reset(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)

        case <-electionTimer.C:
            n.mu.RLock()
            leader := n.leaderID
            n.mu.RUnlock()
            if leader == -1 {
                n.log("ElectionTimeout sin líder, lanzando nueva elección")
                n.startElection()
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

    n.log("Iniciando elección")

    if n.leaderID != -1 && n.leaderID != n.cfg.SelfID {
        n.log("Ya hay un líder registrado (%d), no inicio elección", n.leaderID)
        return
    }

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

WAIT:
    for i := 0; i < len(higherNodes); i++ {
        select {
        case ok := <-responses:
            if ok {
                receivedResponse = true
            }
        case <-timeout:
            break WAIT
        }
    }

    if receivedResponse {
        n.log("Esperando que un nodo con mayor ID se proclame líder...")
        return
    }

    n.becomeLeader()
}

func (n *Node) becomeLeader() {
    n.isLeader = true
    if n.leaderID == n.cfg.SelfID {
        n.log("Ya soy el líder. Ignorando proclamación redundante.")
        return
    }
    
    n.leaderID = n.cfg.SelfID
    n.log("¡Elegido como nuevo líder!")

    msg := &transport.Envelope{
        Type: "LeaderAnnouncement",
        From: n.cfg.SelfID,
    }

    if err := n.transport.Broadcast(msg); err != nil {
        n.log("Error anunciando liderazgo: %v", err)
    }

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

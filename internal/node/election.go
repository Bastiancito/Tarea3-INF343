package node

import (
    "time"
    "github.com/Bastiancito/tarea3/internal/transport"
)

// Ejecuta el ciclo de elecciones si no hay líder o el actual falla.
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

        case <-n.electionCh: // señal para reiniciar temporizador de elección
            if !electionTimer.Stop() {
                <-electionTimer.C
            }
            electionTimer.Reset(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)

        case <-n.ctx.Done():
            return
        }
    }
}

// Inicia el algoritmo del matón
func (n *Node) startElection() {
    n.mu.Lock()
    defer n.mu.Unlock()

    n.log("Iniciando elección")

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

    // Si nadie responde, me autoproclamo
    n.becomeLeader()
}

// Lógica de asumir liderazgo y avisar a los demás
func (n *Node) becomeLeader() {
    n.isLeader = true
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

// Obtiene nodos con mayor ID que yo
func (n *Node) getHigherNodes() []int {
    var higher []int
    for id := range n.cfg.Peers {
        if id > n.cfg.SelfID {
            higher = append(higher, id)
        }
    }
    return higher
}

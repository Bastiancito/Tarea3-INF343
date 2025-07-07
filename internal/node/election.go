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
    n.log("Timeout sin latidos del líder, iniciando elección")

    higher := make([]int, 0, len(n.cfg.Peers))
    for peerID := range n.cfg.Peers {
        if peerID > n.cfg.SelfID {
            higher = append(higher, peerID)
        }
    }

    if len(higher) == 0 {
        n.becomeLeader()
        return
    }

    responses := make(chan struct{}, len(higher))
    for _, pid := range higher {
        go func(id int) {
            ok, _ := n.rpcClient.Election(id)
            if ok {
                responses <- struct{}{}
            }
        }(pid)
    }

    timer := time.NewTimer(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)
    defer timer.Stop()

    select {
    case <-responses:
        n.log("Esperando anuncio de líder de mayor ID")
        return
    case <-timer.C:
    }

    n.becomeLeader()
}

func (n *Node) becomeLeader() {
    n.mu.Lock()
    n.isLeader = true
    n.leaderID = n.cfg.SelfID
    n.mu.Unlock()
    n.resetElectionTimer()
    go func(){
        n.log("Lider %d recuperando estado de peers tras eleccion", n.cfg.SelfID)
        n.syncState()
    }()

    n.log("¡Elegido como nuevo líder!")
    _ = n.transport.Broadcast(&transport.Envelope{
            Type: transport.EnvelopeTypeCoordinator,
            From: n.cfg.SelfID,
        })  


    
    

}

func (n *Node) getHigherPeers() []int {
    var higher []int
    for peerID := range n.cfg.Peers {
        if peerID > n.cfg.SelfID {
            higher = append(higher, peerID)
        }
    }
    return higher
}
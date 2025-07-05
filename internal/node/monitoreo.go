package node

import (
    "time"
)


func (n *Node) monitorLeader() {
    ticker := time.NewTicker(time.Duration(n.cfg.HeartbeatMs) * time.Millisecond)
    defer ticker.Stop()

    timeout := time.NewTimer(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)
    defer timeout.Stop()

    for {
        select {
        case <-ticker.C:
            if n.leaderID != -1 && n.leaderID != n.cfg.SelfID {
                alive, err := n.rpcClient.Ping(n.leaderID)
                if err != nil || !alive {
                    n.log("El líder %d no responde - iniciando elección", n.leaderID)
                    n.electionCh <- struct{}{}
                }
            }

        case <-n.heartbeatCh:
            timeout.Reset(time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond)

        case <-timeout.C:
            n.log("Timeout sin latidos del líder, iniciando elección")
            n.electionCh <- struct{}{}

        case <-n.ctx.Done():
            return
        }
    }
}


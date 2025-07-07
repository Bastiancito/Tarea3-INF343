package node

import (
    "encoding/json"
    "fmt"
    "time"

    "github.com/google/uuid"
    "github.com/Bastiancito/tarea3/internal/transport"
)

func (n *Node) StartEventSimulation(interval time.Duration) {
    ticker := time.NewTicker(interval)
    go func() {
        defer ticker.Stop()
        for {
            select {
            case <-n.ctx.Done():
                return
            case <-ticker.C:
                n.mu.RLock()
                isLeader := n.isLeader
                leaderID := n.leaderID
                n.mu.RUnlock()

                value := fmt.Sprintf("SimEvent-%s", uuid.New().String()[:8])
                n.log("Generando evento simulado: %s", value)

                if isLeader {
                    seq, err := n.ProcessEvent(value)
                    if err != nil {
                        n.log("Error simulando evento: %v", err)
                    } else {
                        n.log("Evento simulado procesado localmente (seq=%d)", seq)
                    }
                } else {
                    n.log("Enviando evento simulado al primario %d: %s", leaderID, value)
                    req := struct{ Value string }{Value: value}
                    data, _ := json.Marshal(req)
                    if err := n.transport.Send(leaderID, &transport.Envelope{
                        Type: transport.EnvelopeTypeSubmitEvent,
                        From: n.cfg.SelfID,
                        Data: data,
                    }); err != nil {
                        n.log("SubmitEvent rechazado(no soy líder): %v", err)
                    }
                }
            }
        }
    }()
}

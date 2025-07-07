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
                value := fmt.Sprintf("SimEvent-%s", uuid.New().String()[:8])
                n.log("Generando evento simulado: %s", value)

                if n.isLeader {
                    seq, err := n.ProcessEvent(value)
                    if err != nil {
                        n.log("Error simulando evento: %v", err)
                    } else {
                        n.log("Evento simulado procesado localmente (seq=%d)", seq)
                    }
                } else {
					n.log("Enviando evento simulado a líder: %s", value)
                    req := struct{ Value string }{Value: value}
                    data, _ := json.Marshal(req)
                    _ = n.transport.Send(n.leaderID, &transport.Envelope{
                        Type: transport.EnvelopeTypeSubmitEvent,
                        From: n.cfg.SelfID,
                        Data: data,
                    })
                }
            }
        }
    }()
}

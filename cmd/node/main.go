package main

import (
    "flag"
    "log"

    "github.com/Bastiancito/tarea3/internal/node"
)

func main() {
    cfgPath := flag.String("config", "conf.yaml", "ruta al archivo de configuración")
    selfID := flag.Int("self_id", 1, "ID numérico de este nodo")
    transport := flag.String("transport", "rpc", "inmem|rpc")
    flag.Parse()

    n, err := node.New(*selfID, *cfgPath, *transport)
    if err != nil {
        log.Fatalf("init error: %v", err)
    }
    if err := n.Start(); err != nil {
        log.Fatalf("run error: %v", err)
    }
}
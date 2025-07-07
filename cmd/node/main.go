package main

import (
    "flag"
    "log"
    "os"
    "os/signal"
    "syscall"
    "github.com/Bastiancito/tarea3/internal/node"
)

func main() {
    id := flag.Int("self_id", 0, "ID del nodo (1,2 o 3)")
    cfgPath := flag.String("config", "", "ruta al archivo confN.yaml")
    transportKind := flag.String("transport", "rpc", "tipo de transporte: rpc|inmem")
    flag.Parse()

    if *id < 1 || *id > 3 || *cfgPath == "" {
        flag.Usage()
        os.Exit(1)
    }

    nd, err := node.New(*id, *cfgPath, *transportKind)
    if err != nil {
        log.Fatalf("New node: %v", err)
    }

    sigs := make(chan os.Signal, 1)
    signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
    <- sigs


    log.Println("Deteniendo nodo...")
    nd.Stop()
}
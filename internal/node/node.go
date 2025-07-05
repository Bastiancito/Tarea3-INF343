package node

import (
  "context"
  "encoding/json"
  "fmt"
  "io"
  "log"
  "os"
  "sync"
  "time"

  "github.com/google/uuid"
  "github.com/Bastiancito/tarea3/internal/transport"
)


type Config struct {
    SelfID            int            `yaml:"self_id"`
    ListenAddr        string         `yaml:"listen_addr"`
    Peers             map[int]string `yaml:"peers"`
    StateFile         string         `yaml:"state_file"`
    HeartbeatMs       int            `yaml:"heartbeat_ms"`
    ElectionTimeoutMs int            `yaml:"election_timeout_ms"`
}


type PersistentState struct {
    Sequence uint64        `json:"sequence_number"`
    Log      []EventRecord `json:"event_log"`
}


type EventRecord struct {
    ID    uuid.UUID `json:"id"`
    Value string    `json:"value"`
}

type Node struct {
    cfg        Config
    mu         sync.RWMutex
    transport  transport.Transport
    state      *PersistentState
    isLeader   bool
    leaderID   int
    electionCh chan struct{}
    heartbeatCh chan struct{}
    ctx        context.Context
    cancel     context.CancelFunc
    logger     *log.Logger
    rpcClient  *RPCClient
}


func initLogger(nodeID int) *log.Logger {
    logFile, err := os.OpenFile(fmt.Sprintf("nodo%d.log", nodeID), 
        os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatal(err)
    }
    
    return log.New(io.MultiWriter(os.Stdout, logFile), 
        "", log.LstdFlags)
}


func (n *Node) log(format string, args ...interface{}) {
    n.logger.Printf("[Nodo %d] "+format, append([]interface{}{n.cfg.SelfID}, args...)...)
}

func New(id int, cfgPath, transportKind string) (*Node, error) {
    cfg, err := loadConfig(cfgPath)
    if err != nil {
        return nil, err
    }
    if cfg.SelfID != id {
        cfg.SelfID = id 
    }

    ctx, cancel := context.WithCancel(context.Background())
    
    var t transport.Transport
    switch transportKind {
    case "inmem":
        t = transport.NewInMemory(cfg.SelfID)
    default:
        t = transport.NewRPC(cfg.SelfID, cfg.ListenAddr, cfg.Peers)
    }

    ps, err := loadState(cfg.StateFile)
    if err != nil {
        return nil, err
    }
    client := NewRPCClient(cfg.SelfID, cfg.Peers)
    node := &Node{
        cfg:         cfg,
        transport:   t,
        state:       ps,
        ctx:         ctx,
        cancel:      cancel,
        electionCh:  make(chan struct{}, 1),
        heartbeatCh: make(chan struct{}, 1),
        rpcClient:   client,
        leaderID:    -1,
        logger:      initLogger(cfg.SelfID),
    }

    node.log("Iniciando nodo (Transporte: %s)", transportKind)
    return node, nil
}


func (n *Node) Start() error {
    n.log("Nodo %d arrancando en %s", n.cfg.SelfID, n.cfg.ListenAddr)

    // 1) Servidor RPC / InMem
    go func() {
        if err := n.transport.Start(); err != nil {
            n.log("Error al arrancar transporte: %v", err)
            os.Exit(1)
        }
    }()

    n.syncState()

    go n.runLeaderElection()

    go n.monitorLeader()

    go n.periodicStateSave()

    go n.handleIncomingMessages()
    

    return nil
}

func (n *Node) Stop() {
    n.cancel()
    n.transport.Close()
    n.state.Save(n.cfg.StateFile)
}

func (n *Node) handleIncomingMessages() {
    for msg := range n.transport.Receive() {
        switch msg.Type {

        case "Ping":
            _ = n.transport.Send(msg.From, &transport.Envelope{
                Type: "PingResponse",
                From: n.cfg.SelfID,
            })

        case "PingResponse":
            select { case n.heartbeatCh <- struct{}{}: default: }

        case "Election":
            if msg.From < n.cfg.SelfID {
                _ = n.transport.Send(msg.From, &transport.Envelope{
                    Type: "Coordinator",
                    From: n.cfg.SelfID,
                })
            }

        case "Coordinator":
            n.mu.Lock()
            prev := n.leaderID
            n.leaderID = msg.From
            n.isLeader = false
            n.mu.Unlock()
            if prev != msg.From {
                n.log("Nodo %d se ha proclamado como líder", msg.From)
            }
            select { case n.heartbeatCh <- struct{}{}: default: }

        case "RequestState":
            data, err := json.Marshal(n.state.Log)
            if err != nil {
                n.log("Error al serializar estado: %v", err)
                continue
            }
            _ = n.transport.Send(msg.From, &transport.Envelope{
                Type: "StateResponse",
                From: n.cfg.SelfID,
                Data: data,
            })

        case "StateResponse":
            var recovered []EventRecord
            if err := json.Unmarshal(msg.Data, &recovered); err != nil {
                n.log("Error al deserializar StateResponse: %v", err)
                continue
            }
            for _, ev := range recovered {
                n.applyEvent(ev)
            }
            select { case n.heartbeatCh <- struct{}{}: default: }

        case "Replicate":
            var ev EventRecord
            if err := json.Unmarshal(msg.Data, &ev); err != nil {
                n.log("Error al decodificar Replicate: %v", err)
                continue
            }
            n.mu.Lock()
            n.state.Sequence = msg.Seq
            n.state.Log = append(n.state.Log, ev)
            n.mu.Unlock()
        }
    }
}

func (n *Node) tryReintegration() {
    n.mu.RLock()
    leaderID := n.leaderID
    n.mu.RUnlock()

    if leaderID == -1 || leaderID == n.cfg.SelfID {
        return
    }

    n.log("Solicitando estado actual al líder %d...", leaderID)

    msg := &transport.Envelope{
        Type: "RequestState",
        From: n.cfg.SelfID,
    }

    if err := n.transport.Send(leaderID, msg); err != nil {
        n.log("Error solicitando estado al líder %d: %v", leaderID, err)
    }
}

func (n *Node) applyEvent(ev EventRecord) {
  n.mu.Lock()
  defer n.mu.Unlock()
  n.state.Sequence = uint64(len(n.state.Log)) + 1
  n.state.Log = append(n.state.Log, ev)
  n.log("Evento aplicado: %s (seq=%d)", ev.Value, n.state.Sequence)
}
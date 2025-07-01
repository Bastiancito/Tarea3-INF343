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
    cfg         Config
    mu          sync.RWMutex
    transport   transport.Transport
    state       *PersistentState
    isLeader    bool
    leaderID    int
    electionCh  chan struct{}
    heartbeatCh chan struct{}
    ctx         context.Context
    cancel      context.CancelFunc
    logger      *log.Logger
}

func initLogger(nodeID int) *log.Logger {
    file, err := os.OpenFile(fmt.Sprintf("nodo%d.log", nodeID), os.O_APPEND|os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        log.Fatal(err)
    }
    return log.New(io.MultiWriter(os.Stdout, file), "", log.LstdFlags)
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

    n := &Node{
        cfg:         cfg,
        transport:   t,
        state:       ps,
        ctx:         ctx,
        cancel:      cancel,
        electionCh:  make(chan struct{}, 1),
        heartbeatCh: make(chan struct{}, 1),
        leaderID:    -1,
        logger:      initLogger(cfg.SelfID),
    }
    n.log("Iniciando nodo (Transporte: %s)", transportKind)
    return n, nil
}

func (n *Node) Start() error {
    if err := n.transport.Start(); err != nil {
        return err
    }
    if rpcTrans, ok := n.transport.(interface{ Receive() <-chan *transport.Envelope }); ok {
        go n.handleIncomingMessages(rpcTrans.Receive())
    }
    go n.monitorLeader()
    go n.periodicStateSave()

    maxID := 0
    for pid := range n.cfg.Peers {
        if pid > maxID {
            maxID = pid
        }
    }
    offsetSteps := maxID - n.cfg.SelfID
    initialDelay := time.Duration(offsetSteps) * time.Duration(n.cfg.ElectionTimeoutMs) * time.Millisecond
    go func() {
        time.Sleep(initialDelay)
        n.runLeaderElection()
    }()

    <-n.ctx.Done()
    return nil
}

func (n *Node) Stop() {
    n.cancel()
    n.transport.Close()
    n.state.Save(n.cfg.StateFile)
}

func (n *Node) handleIncomingMessages(ch <-chan *transport.Envelope) {
    for msg := range ch {
        switch msg.Type {
        case "Heartbeat":
            n.mu.Lock()
            if n.leaderID != msg.From {
                n.log("Heartbeat recibido de nuevo líder %d", msg.From)
            }
            n.leaderID = msg.From
            n.isLeader = (n.cfg.SelfID == msg.From)
            n.mu.Unlock()
            select {
            case n.heartbeatCh <- struct{}{}:
            default:
            }

        case "LeaderAnnouncement":
            n.mu.Lock()
            prev := n.leaderID
            n.leaderID = msg.From
            n.isLeader = false
            n.mu.Unlock()
            if prev != msg.From {
                n.log("Nodo %d se ha proclamado como líder", msg.From)
            }

        case "Replicate":
            var ev EventRecord
            if err := json.Unmarshal(msg.Data, &ev); err != nil {
                n.log("Error decodificando evento: %v", err)
                continue
            }
            n.mu.Lock()
            n.state.Sequence = msg.Seq
            n.state.Log = append(n.state.Log, ev)
            n.mu.Unlock()
        }
    }
}

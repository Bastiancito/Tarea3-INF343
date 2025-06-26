package node

import (
    "context"
    "encoding/json"
    "log"
    "sync"
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

    return &Node{
        cfg:        cfg,
        transport:  t,
        state:      ps,
        ctx:        ctx,
        cancel:     cancel,
        electionCh: make(chan struct{}),
        heartbeatCh: make(chan struct{}),
        leaderID:   -1,
    }, nil
}

func (n *Node) Start() error {
    if err := n.transport.Start(); err != nil {
        return err
    }

    if rpcTrans, ok := n.transport.(interface{ Receive() <-chan *transport.Envelope }); ok {
        go n.handleIncomingMessages(rpcTrans.Receive())
    }
    
    go n.runLeaderElection()
    go n.monitorLeader()
    go n.periodicStateSave()
    
    <-n.ctx.Done()
    return nil
}

func (n *Node) Stop() {
    n.cancel()
    n.transport.Close()
    n.state.Save(n.cfg.StateFile)
}

func (n *Node) handleIncomingMessages(msgChan <-chan *transport.Envelope) {
    for msg := range msgChan {
        switch msg.Type {
        case "Heartbeat":
            n.heartbeatCh <- struct{}{}
        case "LeaderAnnouncement":
            n.mu.Lock()
            n.leaderID = msg.From
            n.isLeader = false
            n.mu.Unlock()
        case "Replicate":
            var event EventRecord
            if err := json.Unmarshal(msg.Data, &event); err != nil {
                log.Printf("Error decoding event: %v", err)
                continue
            }
            n.mu.Lock()
            n.state.Sequence = msg.Seq
            n.state.Log = append(n.state.Log, event)
            n.mu.Unlock()
        }
    }
}
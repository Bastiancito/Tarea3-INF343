package node

import (
    "encoding/json"
    "os"
    "sync"
    "gopkg.in/yaml.v3"
    "github.com/google/uuid"
    "github.com/Bastiancito/tarea3/internal/transport"
)

type Config struct {
    SelfID             int               `yaml:"self_id"`
    ListenAddr         string            `yaml:"listen_addr"`
    Peers              map[int]string    `yaml:"peers"`
    StateFile          string            `yaml:"state_file"`
    HeartbeatMs        int               `yaml:"heartbeat_ms"`
    ElectionTimeoutMs  int               `yaml:"election_timeout_ms"`
}

type Node struct {
    cfg       Config
    mu        sync.RWMutex
    transport transport.Transport
    state     *PersistentState
}

type PersistentState struct {
    Sequence uint64          `json:"sequence_number"`
    Log      []EventRecord   `json:"event_log"`
}

type EventRecord struct {
    ID    uuid.UUID `json:"id"`
    Value string    `json:"value"`
}

func New(id int, cfgPath, transportKind string) (*Node, error) {
    cfg, err := loadConfig(cfgPath)
    if err != nil {
        return nil, err
    }
    if cfg.SelfID != id {
        cfg.SelfID = id 
    }

    var t transport.Transport
    switch transportKind {
    case "inmem":
        t = transport.NewInMemory(cfg.SelfID)
    default:
        t = transport.NewRPC(cfg.SelfID, cfg.ListenAddr, cfg.Peers)
    }

    ps, _ := loadState(cfg.StateFile)
    return &Node{cfg: cfg, transport: t, state: ps}, nil
}

func (n *Node) Start() error {
    if err := n.transport.Start(); err != nil {
        return err
    }
    select {}
}

func loadConfig(path string) (Config, error) {
    data, err := os.ReadFile(path)
    if err != nil {
        return Config{}, err
    }
    var c Config
    if err := yaml.Unmarshal(data, &c); err != nil {
        return c, err
    }
    return c, nil
}

func loadState(path string) (*PersistentState, error) {
    f, err := os.Open(path)
    if err != nil {
        return &PersistentState{}, nil 
    }
    defer f.Close()
    var ps PersistentState
    if err := json.NewDecoder(f).Decode(&ps); err != nil {
        return nil, err
    }
    return &ps, nil
}

func (ps *PersistentState) Save(path string) error {
    f, err := os.Create(path)
    if err != nil {
        return err
    }
    defer f.Close()
    enc := json.NewEncoder(f)
    enc.SetIndent("", "  ")
    return enc.Encode(ps)
}

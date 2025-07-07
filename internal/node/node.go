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

    go func() {
        if err := n.transport.Start(); err != nil {
            n.log("Error al arrancar transporte: %v", err)
            os.Exit(1)
        }
    }()

    go func(){
        time.Sleep(200 * time.Millisecond) 
        n.tryReintegration()
    }()

    
    go n.handleIncomingMessages()
    n.syncState()
    go n.runLeaderElection()

    go n.monitorLeader()

    go n.periodicStateSave()

    

    n.StartEventSimulation(3 * time.Second)
    

    return nil
}

func (n *Node) Stop() {
    status := struct {
       ID           int    `json:"id"`
       IsPrimary    bool   `json:"is_primary"`
       LastMessage  string `json:"last_message"`
   }{
       ID:          n.cfg.SelfID,
       IsPrimary:   n.isLeader,
       LastMessage: time.Now().Format(time.RFC3339),
   }
   n.log("Estado final: id=%d, is_primary=%t, last_message=%s",
       status.ID, status.IsPrimary, status.LastMessage)
   if b, err := json.MarshalIndent(status, "", "  "); err == nil {
       _ = os.WriteFile(
           fmt.Sprintf("status%d.json", n.cfg.SelfID),
           b, 0644,
       )
   }
    n.cancel()
    n.transport.Close()
    n.state.Save(n.cfg.StateFile)
    n.log("Estado final: secuencia=%d, total eventos=%d",
       n.state.Sequence, len(n.state.Log))
}

func (n *Node) handleIncomingMessages() {
    for msg := range n.transport.Receive() {
        switch msg.Type {

        case transport.EnvelopeTypePing:
            _ = n.transport.Send(msg.From, &transport.Envelope{
                Type: transport.EnvelopeTypePingResponse,
                From: n.cfg.SelfID,
            })

        case transport.EnvelopeTypePingResponse:
            select { case n.heartbeatCh <- struct{}{}: default: }

        case transport.EnvelopeTypeElection:
            if msg.From < n.cfg.SelfID {
                _ = n.transport.Send(msg.From, &transport.Envelope{
                    Type: transport.EnvelopeTypeCoordinator,
                    From: n.cfg.SelfID,
                })
            }

        case transport.EnvelopeTypeCoordinator:
            n.mu.Lock()
            prev := n.leaderID
            n.leaderID = msg.From
            n.isLeader = (msg.From == n.cfg.SelfID )
            n.mu.Unlock()
            n.resetElectionTimer()
            if prev != msg.From {
                n.log("Nodo %d se ha proclamado como líder", msg.From)
            }
            if !n.isLeader{
                go n.tryReintegration()
            }
            select { case n.heartbeatCh <- struct{}{}: default: }

        case transport.EnvelopeTypeRequestState:
            data, err := json.Marshal(n.state.Log)
            if err != nil {
                n.log("Error al serializar estado: %v", err)
                continue
            }
            _ = n.transport.Send(msg.From, &transport.Envelope{
                Type: transport.EnvelopeTypeStateResponse,
                From: n.cfg.SelfID,
                Data: data,
            })

        case transport.EnvelopeTypeStateResponse:
            var recovered []EventRecord
            if err := json.Unmarshal(msg.Data, &recovered); err != nil {
                n.log("Error al deserializar StateResponse: %v", err)
                continue
            }
            for _, ev := range recovered {
                n.applyEvent(ev)
            }
            if err:= n.state.Save(n.cfg.StateFile); err != nil {
                n.log("Error al persistir estado recuperado: %v", err)
            }
            n.log("Estado recuperado de nodo %d con %d eventos", msg.From, len(recovered))
            select { case n.heartbeatCh <- struct{}{}: default: }

        case transport.EnvelopeTypeSubmitEvent:
            var req struct{ Value string }
            if err := json.Unmarshal(msg.Data, &req); err != nil {
                n.log("Error al decodificar SubmitEvent: %v", err)
                continue
            }
            if !n.isLeader {
                continue
            }
            seq,err := n.ProcessEvent(req.Value)
            if err != nil {
                n.log("SubmitEvent rechazado(no soy lider): %v", err)
                continue
            }
            n.log("Evento recibido de %d: %s (seq=%d)", msg.From, req.Value, seq)


        case transport.EnvelopeTypeReplicate:
            var ev EventRecord
            if err := json.Unmarshal(msg.Data, &ev); err != nil {
                n.log("Error al decodificar Replicate: %v", err)
                continue
            }
            n.applyEvent(ev)
            n.log("Evento replicado de %d: %s (seq=%d)", msg.From, ev.Value, msg.Seq)
            if err := n.state.Save(n.cfg.StateFile); err != nil {
                n.log("Error al persistir evento replicado: %v", err)
            }


        default:
            n.log("Mensaje desconocido de tipo %q de nodo %d", msg.Type, msg.From)
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
  for _, existing := range n.state.Log {
    if existing.ID == ev.ID {
      n.log("Evento ya existe en el log: %s (seq=%d)", ev.Value, n.state.Sequence)
      return
    }
  }

  n.state.Log = append(n.state.Log, ev)
  n.state.Sequence = uint64(len(n.state.Log))
  n.log("Evento aplicado: %s (seq=%d)", ev.Value, n.state.Sequence)
}
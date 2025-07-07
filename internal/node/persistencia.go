package node

import (
    "encoding/json"
    "log"
    "os"
    "time"
    "gopkg.in/yaml.v2"
)


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
    if _, err := os.Stat(path); os.IsNotExist(err) {
        return &PersistentState{
            Sequence: 0,
            Log:      make([]EventRecord, 0),
        }, nil
    }

    f, err := os.Open(path)
    if err != nil {
        return nil, err
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

func (n *Node) periodicStateSave() {
    ticker := time.NewTicker(5 * time.Second)
    defer ticker.Stop()

    for {
        select {
        case <-ticker.C:
            n.mu.RLock()
            err := n.state.Save(n.cfg.StateFile)
            n.mu.RUnlock()
            
            if err != nil {
                log.Printf("Error saving state: %v", err)
            }
        case <-n.ctx.Done():
            return
        }
    }
}

func (n *Node) syncState() {
    for peerID := range n.cfg.Peers {
        if peerID == n.cfg.SelfID {
            continue
        }
        if err := n.rpcClient.RequestState(peerID, n.state.Sequence); err != nil {
            n.log("Error solicitando estado a %d: %v", peerID, err)
        }
        }
    }

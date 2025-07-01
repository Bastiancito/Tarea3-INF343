#!/usr/bin/env bash

DIR="$( cd "$( dirname "${BASH_SOURCE[0]}" )" && pwd )/.."

ID=$1
if [ -z "$ID" ]; then
  echo "Uso: $0 <id>"
  exit 1
fi

CONFIG="$DIR/conf${ID}.yaml"
if [ ! -f "$CONFIG" ]; then
  echo "❌ No encontré el fichero de config $CONFIG"
  exit 1
fi

LOG="$DIR/node${ID}.log"
PIDFILE="$DIR/node_${ID}.pid"

if [ -f "$PIDFILE" ]; then
  kill "$(cat $PIDFILE)" 2>/dev/null || true
  rm -f "$PIDFILE"
fi

nohup "$DIR/bin/node" \
  -self_id "$ID" \
  -config "$CONFIG" \
  -transport rpc \
  > "$LOG" 2>&1 &

echo $! > "$PIDFILE"
echo "▶ Nodo $ID arrancado (PID=$(cat $PIDFILE)), usando $CONFIG"

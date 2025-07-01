if [ $# -ne 1 ]; then
  echo "Uso: $0 <node_id>"
  exit 1
fi

NODE=$1
PID=$(ps aux | grep "./bin/node -self_id $NODE" | grep -v grep | awk '{print $2}')

if [ -z "$PID" ]; then
  echo "Nodo $NODE no está corriendo"
  exit 1
fi

kill -9 $PID
echo "Nodo $NODE detenido (PID: $PID)"
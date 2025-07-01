SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
for ID in 1 2 3; do
  echo "▶ Arrancando nodo $ID…"
  "$SCRIPT_DIR"/start.sh $ID
  sleep 0.1
done
echo "▶ Todos los nodos arrancados. Comprueba logs con tail -f node1.log node2.log node3.log"
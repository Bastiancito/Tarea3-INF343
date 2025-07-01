Al entrar a las máquinas
Hacer cd Tarea3-INF343

Para iniciar los nodos, en cada MV.
pkill -f "bin/node"
rm -f node_*.pid
chmod +x scripts/start.sh scripts/start_all.sh scripts/simulate_fail.sh

Luego por cada MV:
./scripts/start.sh 2

./scripts/start.sh 1

./scripts/start.sh 3

y para ver los logs:
tail -f node1.log

tail -f node2.log

tail -f node3.log


Para detener algún nodo:

cd script

./simulate_fail.sh 3

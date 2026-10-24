#!/usr/bin/env bash
set -e

echo "=== TickRaft End-to-End Synthetic Demonstration ==="

mkdir -p bin data/demo_cluster/node1 data/demo_cluster/node2 data/demo_cluster/node3 data/export results

# 1. Compile Binaries
echo "[1/6] Compiling Go binaries..."
go build -o bin/tickraft-node ./cmd/tickraft-node
go build -o bin/tickraft-import ./cmd/tickraft-import
go build -o bin/tickraft-export ./cmd/tickraft-export
go build -o bin/tickraftctl ./cmd/tickraftctl

# 2. Launch 3-Node Raft Cluster
echo "[2/6] Launching 3 Raft cluster nodes..."
./bin/tickraft-node -id=node-1 -listen=:50051 -peers=node-2=127.0.0.1:50052,node-3=127.0.0.1:50053 -data-dir=data/demo_cluster/node1 &
PID1=$!
./bin/tickraft-node -id=node-2 -listen=:50052 -peers=node-1=127.0.0.1:50051,node-3=127.0.0.1:50053 -data-dir=data/demo_cluster/node2 &
PID2=$!
./bin/tickraft-node -id=node-3 -listen=:50053 -peers=node-1=127.0.0.1:50051,node-2=127.0.0.1:50052 -data-dir=data/demo_cluster/node3 &
PID3=$!

cleanup() {
  echo "Shutting down cluster..."
  kill $PID1 $PID2 $PID3 2>/dev/null || true
}
trap cleanup EXIT

sleep 3

# 3. Query Cluster Status
echo "[3/6] Querying cluster status..."
./bin/tickraftctl status

# 4. Import synthetic events
echo "[4/6] Importing synthetic event batches..."
./bin/tickraft-import -dataset-id=demo-dataset-synth -batch-size=4 -message-file=data/fixtures/synthetic_message.csv -book-file=data/fixtures/synthetic_orderbook.csv

# 5. Export committed replay
echo "[5/6] Exporting committed replay..."
./bin/tickraft-export -dataset-id=demo-dataset-synth -out-dir=data/export

# 6. Run research pipeline
echo "[6/6] Running research pipeline..."
export PYTHONPATH="python"
python python/tickraft_research/reports/build_report.py --export-csv=data/export/demo-dataset-synth_committed_replay.csv --manifest=data/export/demo-dataset-synth_manifest.json --output-dir=results --horizons=1,2 --embargo-seconds=1

echo "Demo finished successfully!"

# TickRaft End-to-End Synthetic Demonstration Script (PowerShell)
# Requires no external credentials or private market data.

$ErrorActionPreference = "Stop"

Write-Host "==========================================================" -ForegroundColor Cyan
Write-Host "         TickRaft End-to-End System & Research Demo       " -ForegroundColor Cyan
Write-Host "==========================================================" -ForegroundColor Cyan

$dataBaseDir = "data/demo_cluster"
if (Test-Path $dataBaseDir) {
    Remove-Item -Recurse -Force $dataBaseDir
}
New-Item -ItemType Directory -Force -Path "$dataBaseDir/node1", "$dataBaseDir/node2", "$dataBaseDir/node3", "data/export", "results" | Out-Null

# 1. Compile Binaries
Write-Host "`n[1/6] Compiling Go binaries..." -ForegroundColor Yellow
go build -o bin/tickraft-node.exe ./cmd/tickraft-node
go build -o bin/tickraft-import.exe ./cmd/tickraft-import
go build -o bin/tickraft-export.exe ./cmd/tickraft-export
go build -o bin/tickraftctl.exe ./cmd/tickraftctl
Write-Host "Binaries compiled successfully." -ForegroundColor Green

# 2. Launch 3-Node Raft Cluster
Write-Host "`n[2/6] Launching 3 Raft cluster nodes on localhost:50051, 50052, 50053..." -ForegroundColor Yellow
$p1 = Start-Process -FilePath "bin/tickraft-node.exe" -ArgumentList "-id=node-1 -listen=:50051 -peers=node-2=127.0.0.1:50052,node-3=127.0.0.1:50053 -data-dir=$dataBaseDir/node1" -PassThru -NoNewWindow
$p2 = Start-Process -FilePath "bin/tickraft-node.exe" -ArgumentList "-id=node-2 -listen=:50052 -peers=node-1=127.0.0.1:50051,node-3=127.0.0.1:50053 -data-dir=$dataBaseDir/node2" -PassThru -NoNewWindow
$p3 = Start-Process -FilePath "bin/tickraft-node.exe" -ArgumentList "-id=node-3 -listen=:50053 -peers=node-1=127.0.0.1:50051,node-2=127.0.0.1:50052 -data-dir=$dataBaseDir/node3" -PassThru -NoNewWindow

Start-Sleep -Seconds 3

# 3. Query Cluster Status
Write-Host "`n[3/6] Querying initial cluster status via tickraftctl..." -ForegroundColor Yellow
& "bin/tickraftctl.exe" status

# 4. Ingest Synthetic Market-Data Batches
Write-Host "`n[4/6] Importing synthetic event batches into Raft log..." -ForegroundColor Yellow
& "bin/tickraft-import.exe" "-dataset-id=demo-dataset-synth" "-batch-size=4" "-message-file=data/fixtures/synthetic_message.csv" "-book-file=data/fixtures/synthetic_orderbook.csv"

# 5. Export Committed Replay & Verify Manifest
Write-Host "`n[5/6] Exporting committed replay from leader..." -ForegroundColor Yellow
& "bin/tickraft-export.exe" "-dataset-id=demo-dataset-synth" "-out-dir=data/export"

# 6. Run Microstructure Research Pipeline on Committed Replay
Write-Host "`n[6/6] Executing Python research pipeline on committed replay..." -ForegroundColor Yellow
$env:PYTHONPATH = "python"
python python/tickraft_research/reports/build_report.py "--export-csv=data/export/demo-dataset-synth_committed_replay.csv" "--manifest=data/export/demo-dataset-synth_manifest.json" "--output-dir=results" "--horizons=1,2" "--embargo-seconds=1"

# Shutdown Cluster
Write-Host "`nCleaning up cluster processes..." -ForegroundColor Yellow
Stop-Process -Id $p1.Id, $p2.Id, $p3.Id -Force -ErrorAction SilentlyContinue

Write-Host "`n==========================================================" -ForegroundColor Green
Write-Host "Demo completed successfully!" -ForegroundColor Green
Write-Host "Committed Export: data/export/demo-dataset-synth_committed_replay.csv"
Write-Host "Research Output: results/research_summary.json"
Write-Host "==========================================================" -ForegroundColor Green

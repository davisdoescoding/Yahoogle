# exit on error
$ErrorActionPreference = "Stop"

# func to check if a port is open and accepting connections
function Wait-ForPort {
    param (
        [string]$Host,
        [int]$Port,
        [int]$Timeout = 30
    )
    Write-Host "Waiting for $Host:$Port to be ready..."
    for ($i = 0; $i -lt $Timeout; $i++) {
        if (Test-NetConnection -ComputerName $Host -Port $Port -InformationLevel Quiet) {
            Write-Host "$Host:$Port is ready."
            return
        }
        Start-Sleep -Seconds 1
    }
    Write-Error "Error: $Host:$Port did not become ready in time."
    exit 1
}

# compile Java source files
Write-Host "Compiling Java source files..."
javac -sourcepath src -d bin (Get-ChildItem -Path src -Recurse -Filter *.java).FullName
Write-Host "Compilation complete."
Start-Sleep -Seconds 2

# start KVS Coordinator
Write-Host "Starting KVS Coordinator on port 8000..."
Start-Process -FilePath "java" -ArgumentList "-cp bin cis5550.kvs.Coordinator 8000" -NoNewWindow -PassThru | Out-Null
Wait-ForPort -Host "localhost" -Port 8000

# start KVS Workers
for ($i = 1; $i -le 10; $i++) {
    $port = 8000 + $i
    $worker = "worker$i"
    Write-Host "Starting KVS Worker $i on port $port..."
    Start-Process -FilePath "java" -ArgumentList "-cp bin cis5550.kvs.Worker $port $worker localhost:8000" -NoNewWindow -PassThru | Out-Null
    Wait-ForPort -Host "localhost" -Port $port
}

# start Flame Coordinator
Write-Host "Starting Flame Coordinator on port 9000..."
Start-Process -FilePath "java" -ArgumentList "-cp bin cis5550.flame.Coordinator 9000 localhost:8000" -NoNewWindow -PassThru | Out-Null
Wait-ForPort -Host "localhost" -Port 9000

# start Flame Workers
for ($i = 1; $i -le 10; $i++) {
    $port = 9000 + $i
    Write-Host "Starting Flame Worker $i on port $port..."
    Start-Process -FilePath "java" -ArgumentList "-cp bin cis5550.flame.Worker $port localhost:9000" -NoNewWindow -PassThru | Out-Null
    Wait-ForPort -Host "localhost" -Port $port
}

Write-Host "All processes started successfully!"

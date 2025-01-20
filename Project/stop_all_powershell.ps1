Write-Host "Stopping all Java processes..."
Get-Process java | Stop-Process -Force
Write-Host "All Java processes have been stopped."

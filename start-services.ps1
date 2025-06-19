# Start TextBlob service in a new window
Start-Process powershell -ArgumentList "-NoExit", "-Command", "cd '$PSScriptRoot'; python textblob_server.py"

# Wait a moment for TextBlob service to start
Start-Sleep -Seconds 2

# Start the main application with cache
& "$PSScriptRoot\start-cached.ps1" 
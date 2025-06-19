# Kill existing processes on ports 5000 and 5001
Write-Host "Checking for existing processes..."
$processes = netstat -ano | findstr ":5000 :5001"
foreach ($process in $processes) {
    $pid = ($process -split ' +')[-1]
    taskkill /F /PID $pid 2>$null
}
Write-Host "Ports cleared"

# Start TextBlob service
Write-Host "Starting TextBlob service..."
$env:PYTHONUNBUFFERED = "1"
Start-Process python -ArgumentList "textblob_server.py" -NoNewWindow

# Wait for service to start
Write-Host "Waiting for TextBlob service to initialize..."
Start-Sleep -Seconds 5

# Check if cache exists
$cachePath = Join-Path $PSScriptRoot "data" "sentiment-cache.json"
if (Test-Path $cachePath) {
    Write-Host "Using existing sentiment cache"
} else {
    Write-Host "No cache found - application will create one on first run"
}

# Build and start
Write-Host "Building and starting application..."
npm run build
npx esbuild server/sentiment-analyzer.ts --platform=node --packages=external --bundle --format=esm --outfile=server/sentiment-analyzer.js
npm run start-with-cache 
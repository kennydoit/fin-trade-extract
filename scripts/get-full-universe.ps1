# Requires ALPHAVANTAGE_API_KEY in environment
param(
    [string]$OutFile = "data/symbols/full_universe.csv",
    [switch]$StocksOnly
)

Write-Host "Fetching full symbol universe from Alpha Vantage..." -ForegroundColor Cyan

if (-not $env:ALPHAVANTAGE_API_KEY) {
    Write-Error "ALPHAVANTAGE_API_KEY not set in environment. Please set it and retry."
    exit 1
}

$outPath = Join-Path -Path (Get-Location) -ChildPath $OutFile
$outDir = Split-Path $outPath -Parent
if (-not (Test-Path $outDir)) { New-Item -ItemType Directory -Force -Path $outDir | Out-Null }

$url = "https://www.alphavantage.co/query?function=LISTING_STATUS&apikey=$($env:ALPHAVANTAGE_API_KEY)"
Write-Host "GET $url" -ForegroundColor DarkGray

try {
    $response = Invoke-WebRequest -Uri $url -UseBasicParsing -TimeoutSec 120
    $csv = $response.Content

    if ([string]::IsNullOrWhiteSpace($csv)) {
        throw "Empty response from Alpha Vantage LISTING_STATUS"
    }

    if ($StocksOnly.IsPresent) {
        # Filter rows where assetType == Stock (keep header)
        $lines = $csv -split "`n"
        if ($lines.Length -gt 1) {
            $header = $lines[0]
            $body = $lines[1..($lines.Length-1)] | Where-Object { $_ -match ",Stock,|,Common Stock,|,Preferred Stock," }
            $csv = ($header, $body) -join "`n"
        }
    }

    $csv | Out-File -FilePath $outPath -Encoding utf8 -Force
    Write-Host "Saved: $outPath" -ForegroundColor Green
}
catch {
    Write-Error "Failed to fetch symbols: $_"
    exit 2
}

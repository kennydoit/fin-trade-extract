# ============================================================================
# LAMBDA DEVELOPMENT TOOLKIT - Local VS Code Edition
# Tools for easier Lambda function development, testing, and monitoring
# ============================================================================

param(
    [Parameter(Mandatory=$true, Position=0)]
    [ValidateSet("test", "logs", "deploy", "status", "invoke", "monitor")]
    [string]$Action,
    
    [Parameter(Mandatory=$false)]
    [string]$FunctionName = "fin-trade-extract-overview",
    
    [Parameter(Mandatory=$false)]
    [string]$Region = "us-east-2",
    
    [Parameter(Mandatory=$false)]
    [string]$LogLines = "50",
    
    [Parameter(Mandatory=$false)]
    [string]$TestPayload = '{"symbols":["AAPL"],"run_id":"dev-test","batch_size":1}',
    
    [Parameter(Mandatory=$false)]
    [switch]$Follow = $false,
    
    [Parameter(Mandatory=$false)]
    [switch]$Verbose = $false
)

# Enhanced output functions
function Write-DevSuccess($message) { Write-Host "✅ $message" -ForegroundColor Green }
function Write-DevInfo($message) { Write-Host "🔧 $message" -ForegroundColor Cyan }
function Write-DevWarning($message) { Write-Host "⚠️  $message" -ForegroundColor Yellow }
function Write-DevError($message) { Write-Host "❌ $message" -ForegroundColor Red }
function Write-DevHeader($message) { 
    Write-Host "`n" + "="*80 -ForegroundColor DarkGray
    Write-Host "🛠️  $message" -ForegroundColor Blue
    Write-Host "="*80 -ForegroundColor DarkGray
}

function Test-LambdaFunction {
    Write-DevHeader "TESTING LAMBDA FUNCTION: $FunctionName"
    
    try {
        # Test with simple payload
        Write-DevInfo "Sending test payload..."
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($TestPayload)
        $base64 = [Convert]::ToBase64String($bytes)
        
        $result = aws lambda invoke `
            --function-name $FunctionName `
            --payload $base64 `
            --region $Region `
            --output json `
            "test-response.json" 2>&1
            
        if ($LASTEXITCODE -ne 0) {
            Write-DevError "Lambda invocation failed: $result"
            return
        }
        
        if (Test-Path "test-response.json") {
            $response = Get-Content "test-response.json" -Raw | ConvertFrom-Json
            
            Write-DevSuccess "Lambda function responded successfully!"
            Write-DevInfo "Status Code: $($response.statusCode)"
            
            if ($response.statusCode -eq 200) {
                Write-DevSuccess "Function execution successful"
                if ($response.success_count) {
                    Write-DevInfo "Processed: $($response.success_count) symbols"
                    Write-DevInfo "Performance: $($response.rate_limiter_stats.estimated_symbols_per_hour) symbols/hour"
                }
            } else {
                Write-DevWarning "Function returned non-200 status"
                if ($response.error) {
                    Write-DevError "Error: $($response.error)"
                }
            }
            
            Remove-Item "test-response.json" -Force
        }
        
    } catch {
        Write-DevError "Test failed: $($_.Exception.Message)"
    }
}

function Get-LambdaLogs {
    param([bool]$FollowLogs = $false)
    
    Write-DevHeader "LAMBDA FUNCTION LOGS: $FunctionName"
    
    try {
        $logGroup = "/aws/lambda/$FunctionName"
        
        if ($FollowLogs) {
            Write-DevInfo "Following logs in real-time (Ctrl+C to stop)..."
            aws logs tail $logGroup --region $Region --follow
        } else {
            Write-DevInfo "Getting last $LogLines log entries..."
            aws logs tail $logGroup --region $Region --since "1h" | Select-Object -Last ([int]$LogLines)
        }
        
    } catch {
        Write-DevError "Failed to retrieve logs: $($_.Exception.Message)"
    }
}

function Deploy-LambdaFunction {
    Write-DevHeader "DEPLOYING LAMBDA FUNCTION: $FunctionName"
    
    $functionPath = "lambda-functions\overview-extractor"
    
    if (-not (Test-Path $functionPath)) {
        Write-DevError "Function path not found: $functionPath"
        return
    }
    
    try {
        Write-DevInfo "Creating deployment package..."
        
        # Create temp deployment directory
        $tempDir = "lambda-deploy-$(Get-Date -Format 'yyyyMMdd-HHmmss')"
        New-Item -ItemType Directory -Path $tempDir -Force | Out-Null
        
        # Install minimal dependencies
        Write-DevInfo "Installing dependencies..."
        $requirementsFile = "lambda-minimal-requirements.txt"
        if (Test-Path $requirementsFile) {
            python -m pip install -r $requirementsFile -t $tempDir --quiet
        }
        
        # Copy function files
        Write-DevInfo "Copying function files..."
        New-Item -ItemType Directory -Path "$tempDir\common" -Force | Out-Null
        Copy-Item -Path "$functionPath\*.py" -Destination $tempDir -Force
        Copy-Item -Path "lambda-functions\common\*.py" -Destination "$tempDir\common\" -Force
        
        # Create zip
        Write-DevInfo "Creating deployment package..."
        Push-Location $tempDir
        Compress-Archive -Path "*" -DestinationPath "..\$tempDir.zip" -Force
        Pop-Location
        
        # Deploy to AWS
        Write-DevInfo "Uploading to AWS Lambda..."
        $deployResult = aws lambda update-function-code `
            --function-name $FunctionName `
            --zip-file "fileb://$tempDir.zip" `
            --region $Region `
            --output json 2>&1
            
        if ($LASTEXITCODE -eq 0) {
            $deployment = $deployResult | ConvertFrom-Json
            Write-DevSuccess "Deployment successful!"
            Write-DevInfo "Function: $($deployment.FunctionName)"
            Write-DevInfo "Runtime: $($deployment.Runtime)"
            Write-DevInfo "Code Size: $([math]::Round($deployment.CodeSize / 1024 / 1024, 2)) MB"
            Write-DevInfo "Last Modified: $($deployment.LastModified)"
        } else {
            Write-DevError "Deployment failed: $deployResult"
        }
        
        # Cleanup
        Remove-Item -Path $tempDir -Recurse -Force -ErrorAction SilentlyContinue
        Remove-Item -Path "$tempDir.zip" -Force -ErrorAction SilentlyContinue
        
    } catch {
        Write-DevError "Deployment failed: $($_.Exception.Message)"
    }
}

function Get-LambdaStatus {
    Write-DevHeader "LAMBDA FUNCTION STATUS: $FunctionName"
    
    try {
        $functionInfo = aws lambda get-function --function-name $FunctionName --region $Region --output json | ConvertFrom-Json
        
        Write-DevSuccess "Function found and accessible"
        Write-DevInfo "Function Name: $($functionInfo.Configuration.FunctionName)"
        Write-DevInfo "Runtime: $($functionInfo.Configuration.Runtime)"
        Write-DevInfo "Handler: $($functionInfo.Configuration.Handler)"
        Write-DevInfo "Memory: $($functionInfo.Configuration.MemorySize) MB"
        Write-DevInfo "Timeout: $($functionInfo.Configuration.Timeout) seconds"
        Write-DevInfo "Code Size: $([math]::Round($functionInfo.Configuration.CodeSize / 1024 / 1024, 2)) MB"
        Write-DevInfo "Last Modified: $($functionInfo.Configuration.LastModified)"
        Write-DevInfo "State: $($functionInfo.Configuration.State)"
        Write-DevInfo "Last Update Status: $($functionInfo.Configuration.LastUpdateStatus)"
        
        if ($functionInfo.Configuration.Environment.Variables) {
            Write-DevInfo "Environment Variables:"
            $functionInfo.Configuration.Environment.Variables | Get-Member -Type NoteProperty | ForEach-Object {
                $key = $_.Name
                $value = $functionInfo.Configuration.Environment.Variables.$key
                Write-DevInfo "  $key = $value"
            }
        }
        
    } catch {
        Write-DevError "Failed to get function status: $($_.Exception.Message)"
    }
}

function Invoke-CustomLambda {
    Write-DevHeader "CUSTOM LAMBDA INVOCATION: $FunctionName"
    
    Write-DevInfo "Using payload: $TestPayload"
    
    try {
        $bytes = [System.Text.Encoding]::UTF8.GetBytes($TestPayload)
        $base64 = [Convert]::ToBase64String($bytes)
        
        Write-DevInfo "Invoking function..."
        $result = aws lambda invoke `
            --function-name $FunctionName `
            --payload $base64 `
            --region $Region `
            --output json `
            "custom-response.json" 2>&1
            
        if ($LASTEXITCODE -eq 0) {
            Write-DevSuccess "Invocation completed"
            
            if (Test-Path "custom-response.json") {
                Write-DevInfo "Response:"
                Get-Content "custom-response.json" -Raw | ConvertFrom-Json | ConvertTo-Json -Depth 10
                Remove-Item "custom-response.json" -Force
            }
        } else {
            Write-DevError "Invocation failed: $result"
        }
        
    } catch {
        Write-DevError "Custom invocation failed: $($_.Exception.Message)"
    }
}

function Start-LambdaMonitor {
    Write-DevHeader "LAMBDA FUNCTION MONITOR: $FunctionName"
    Write-DevInfo "Monitoring function metrics and logs..."
    Write-DevInfo "Press Ctrl+C to stop monitoring"
    
    try {
        while ($true) {
            Clear-Host
            Write-DevHeader "LAMBDA MONITOR - $(Get-Date -Format 'yyyy-MM-dd HH:mm:ss')"
            
            # Get function status
            $status = aws lambda get-function --function-name $FunctionName --region $Region --output json 2>$null
            if ($status) {
                $statusInfo = $status | ConvertFrom-Json
                Write-DevInfo "State: $($statusInfo.Configuration.State) | Status: $($statusInfo.Configuration.LastUpdateStatus)"
            }
            
            # Get recent metrics (simplified)
            Write-DevInfo "Recent activity:"
            aws logs tail "/aws/lambda/$FunctionName" --region $Region --since "5m" --format short 2>$null | Select-Object -Last 5
            
            Write-DevInfo "`nRefreshing in 30 seconds... (Ctrl+C to stop)"
            Start-Sleep 30
        }
    } catch {
        Write-DevInfo "Monitoring stopped"
    }
}

# ============================================================================
# MAIN EXECUTION
# ============================================================================

Write-DevHeader "🛠️  LAMBDA DEVELOPMENT TOOLKIT"
Write-DevInfo "Action: $Action | Function: $FunctionName | Region: $Region"

switch ($Action.ToLower()) {
    "test" { 
        Test-LambdaFunction 
    }
    "logs" { 
        Get-LambdaLogs -FollowLogs $Follow.IsPresent 
    }
    "deploy" { 
        Deploy-LambdaFunction 
    }
    "status" { 
        Get-LambdaStatus 
    }
    "invoke" { 
        Invoke-CustomLambda 
    }
    "monitor" { 
        Start-LambdaMonitor 
    }
    default { 
        Write-DevError "Unknown action: $Action"
        Write-DevInfo "Available actions: test, logs, deploy, status, invoke, monitor"
    }
}

Write-DevHeader "🏁 TOOLKIT OPERATION COMPLETE"
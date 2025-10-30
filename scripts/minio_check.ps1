<#
Helper script: minio_check.ps1

What it does:
- If the project contains a host-mounted MinIO data folder (./minio/bucket), list files there (fast, indirect path).
- Otherwise it runs a temporary minio/mc container which sets an alias and lists the `bronze` bucket. It mounts a small host folder as `/root/.mc` so the mc alias persists between runs (avoids the "Requested path /myminio not found" problem).

Usage (PowerShell):
  .\scripts\minio_check.ps1

Requirements:
- Docker must be running
- The docker-compose network name (default in this repo) is `data-pipeline-network` (adjust if yours is different)
#>

Set-StrictMode -Version Latest
$ErrorActionPreference = 'Stop'

$scriptRoot = Split-Path -Parent $MyInvocation.MyCommand.Path
$repoRoot = Resolve-Path (Join-Path $scriptRoot '..')

# host-minio-data path (project relative)
$hostMinioPath = Join-Path $repoRoot 'minio\bucket'

Write-Host "Checking host-mounted MinIO data path: $hostMinioPath" -ForegroundColor Cyan
if (Test-Path $hostMinioPath) {
    Write-Host "Found host MinIO data. Listing (top-level) ..." -ForegroundColor Green
    Get-ChildItem -Path $hostMinioPath -Directory | ForEach-Object {
        Write-Host " - $($_.FullName)" -ForegroundColor Yellow
        Get-ChildItem -Path $_.FullName -Recurse -File -Depth 2 | Select-Object @{n='Path';e={$_.FullName}},Length | Format-Table -AutoSize
    }
    exit 0
}

Write-Host "Host MinIO data path not found. Will attempt to use dockerized mc (minio client)." -ForegroundColor Yellow

# prepare persistent mc config dir (so alias persists between runs)
$mcConfig = Join-Path $env:TEMP 'mc_config_data_pipeline'
if (-not (Test-Path $mcConfig)) { New-Item -ItemType Directory -Path $mcConfig | Out-Null }

$aliasCmd = 'mc alias set myminio http://minio:9000 minioadmin minioadmin'
$listBronzeCmd = 'mc ls myminio/bronze || mc ls myminio'

Write-Host "Running dockerized mc to set alias and list bucket (using persistent mc config at $mcConfig)" -ForegroundColor Cyan

$args = @(
    'run', '--rm', '-v', "$mcConfig:/root/.mc", '--network', 'data-pipeline-network', 'minio/mc', 'sh', '-c', "$aliasCmd && $listBronzeCmd"
)

# Use the docker executable
try {
    & docker @args
} catch {
    Write-Host "Failed to run dockerized mc: $_" -ForegroundColor Red
    Write-Host "If you're on Windows, ensure Docker Desktop is running and the network 'data-pipeline-network' exists." -ForegroundColor Yellow
    exit 2
}

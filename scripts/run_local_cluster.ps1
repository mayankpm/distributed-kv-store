# Starts every node listed in a cluster config as a background process.
#   scripts\run_local_cluster.ps1 [-Config examples\cluster3.conf] [-Build build]
# Stop them with: Get-Content data\pids | ForEach-Object { Stop-Process -Id $_ }
param(
  [string]$Config = "examples\cluster3.conf",
  [string]$Build = "build"
)
$ErrorActionPreference = "Stop"

New-Item -ItemType Directory -Force data | Out-Null
Set-Content data\pids ""
$ids = Get-Content $Config | Where-Object { $_ -match '^\s*node\s+(\d+)' } | ForEach-Object { $Matches[1] }
foreach ($id in $ids) {
  $p = Start-Process -FilePath "$Build\kvnode.exe" `
    -ArgumentList "--config", $Config, "--id", $id, "--data", "data" `
    -RedirectStandardOutput "data\node$id.log" -RedirectStandardError "data\node$id.err" `
    -NoNewWindow -PassThru
  Add-Content data\pids $p.Id
  Write-Host "started node $id (pid $($p.Id)), log in data\node$id.log"
}
Write-Host "try: $Build\kvcli.exe --config $Config put hello world; $Build\kvcli.exe --config $Config get hello"

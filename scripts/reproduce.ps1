# reproduce.ps1 — rebuild derived results + report inputs from raw evidence
param([string]$RunDir = "")
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root
$env:PYTHONPATH = "src"
if (-not $RunDir) {
    $idx = Get-Content "manifests/run_index.json" | ConvertFrom-Json
    $RunDir = $idx[-1].dir
}
Write-Host "rebuilding derived from $RunDir"
$dest = "derived/" + (Split-Path $RunDir -Leaf)
python -m retry_budget_study.analysis --raw $RunDir --derived $dest --matrix config/pilot_matrix.json
$code = $LASTEXITCODE
python -m retry_budget_study.hash_raw --raw raw --out manifests/raw_hashes.json
exit ($code -or $LASTEXITCODE)

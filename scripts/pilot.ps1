# pilot.ps1 — run exploratory pilot (writes raw/<run_id>/)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root
$env:PYTHONPATH = "src"
$runId = "run_" + (Get-Date -Format "yyyyMMdd_HHmmss")
python -m retry_budget_study.runner --out "raw/$runId" --max-attempts 3000
$code = $LASTEXITCODE
python -m retry_budget_study.analysis --raw "raw/$runId" --derived "derived/$runId" --matrix config/pilot_matrix.json
exit ($code -or $LASTEXITCODE)

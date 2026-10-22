# doctor.ps1 — environment + permission preflight + resource caps
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root
$env:PYTHONPATH = "src"
python -m retry_budget_study.doctor
exit $LASTEXITCODE

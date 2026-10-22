# test.ps1 — python unit tests (harness + analyzer integrity)
$ErrorActionPreference = "Stop"
$root = Split-Path -Parent (Split-Path -Parent $MyInvocation.MyCommand.Path)
Set-Location $root
$env:PYTHONPATH = "src"
python scripts/verify.py @args
exit $LASTEXITCODE

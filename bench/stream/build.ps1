# Builds every server binary into bench/stream/bin.
# Run from bench/stream:  .\build.ps1
$ErrorActionPreference = 'Stop'
$go = "C:\Program Files\Go\bin\go.exe"
$bin = "$env:LOCALAPPDATA\Microsoft\WinGet\Packages\BrechtSanders.WinLibs.POSIX.UCRT_Microsoft.Winget.Source_8wekyb3d8bbwe\mingw64\bin"
$env:Path = "$bin;$env:Path"
$env:CGO_ENABLED = "1"

Set-Location $PSScriptRoot
New-Item -ItemType Directory -Force -Path "bin" | Out-Null

Write-Host "go build ./..." -ForegroundColor Cyan
& $go build ./...
if ($LASTEXITCODE -ne 0) { throw "go build failed" }

foreach ($s in @('photon', 'chi', 'gin', 'echo', 'nethttp')) {
    Write-Host "  building $s" -ForegroundColor DarkGray
    & $go build -o "bin/$s.exe" "./go_servers/$s"
    if ($LASTEXITCODE -ne 0) { throw "build $s failed" }
}

# The driver and the equivalence gate. The gate is built here rather than run
# with `go run` because it is the precondition for every published number, and
# `go run` would rebuild it on each invocation and make a stale binary possible.
foreach ($d in @('equivcheck', 'streamdriver')) {
    $dir = if ($d -eq 'equivcheck') { 'driver/equivcheck' } else { 'driver' }
    Write-Host "  building $d" -ForegroundColor DarkGray
    & $go build -o "bin/$d.exe" "./$dir"
    if ($LASTEXITCODE -ne 0) { throw "build $d failed" }
}

Write-Host "built:" -ForegroundColor Cyan
Get-ChildItem bin | Select-Object Name, Length | Format-Table | Out-String | Write-Host

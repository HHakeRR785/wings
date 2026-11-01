[CmdletBinding()]
param (
    [ValidateSet('amd64', 'arm64', 'all')]
    [string]$Arch = 'amd64',
    [string]$Version = 'v1.13.4-custom',
    [switch]$Clean
)

$ErrorActionPreference = 'Stop'

$BuildDir = Join-Path $PSScriptRoot "build"

if ($Clean) {
    if (Test-Path $BuildDir) {
        Write-Host "Usuwanie katalogu build..." -ForegroundColor Yellow
        Remove-Item -Recurse -Force $BuildDir
    }
    Write-Host "Wyczyszczono." -ForegroundColor Green
    return
}

Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "   Pterodactyl Wings - Skrypt kompilacji     " -ForegroundColor Cyan
Write-Host "=============================================" -ForegroundColor Cyan
Write-Host "Wersja docelowa: $Version" -ForegroundColor Gray

if (-not (Test-Path $BuildDir)) {
    New-Item -ItemType Directory -Path $BuildDir | Out-Null
}

$Targets = if ($Arch -eq 'all') { @('amd64', 'arm64') } else { @($Arch) }

foreach ($targetArch in $Targets) {
    $OutputFile = Join-Path $BuildDir "wings_linux_$targetArch"
    Write-Host "`n[+] Kompilacja dla Linux ($targetArch)..." -ForegroundColor Yellow

    $env:GOOS = "linux"
    $env:GOARCH = $targetArch
    $ldflags = "-s -w -X github.com/pterodactyl/wings/system.Version=$Version"

    & go build -trimpath -ldflags $ldflags -o $OutputFile wings.go

    if ($LASTEXITCODE -eq 0 -and (Test-Path $OutputFile)) {
        $fileSizeMB = [math]::Round((Get-Item $OutputFile).Length / 1MB, 2)
        Write-Host "-> Sukces! Utworzono plik: $OutputFile ($fileSizeMB MB)" -ForegroundColor Green
    } else {
        Write-Error "Kompilacja dla architektury $targetArch zakonczona bledem."
    }
}

Write-Host "`n[V] Gotowe! Skompilowany plik binarny mozesz przeslac na serwer Linux (np. /usr/local/bin/wings)." -ForegroundColor Cyan

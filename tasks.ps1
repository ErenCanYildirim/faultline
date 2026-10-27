<#

.EXAMPLE
  .\tasks.ps1 up              # build images and start the stack
  .\tasks.ps1 down            # stop the stack, keep data
  .\tasks.ps1 down -Volumes   # stop the stack and delete Redpanda/ClickHouse data
  .\tasks.ps1 test            # run Go tests (local Go if installed, otherwise in Docker)
  .\tasks.ps1 logs            # follow logs of all services
  .\tasks.ps1 logs ingest     # follow logs of one service
#>
param(
    [Parameter(Position = 0, Mandatory = $true)]
    [ValidateSet('up', 'down', 'test', 'logs')]
    [string]$Target,

    [Parameter(Position = 1)]
    [string]$Service = '',

    [switch]$Volumes
)

$ErrorActionPreference = 'Stop'
Set-Location -Path $PSScriptRoot

function Invoke-Checked {
    param([string]$Exe, [string[]]$Arguments)
    & $Exe @Arguments
    if ($LASTEXITCODE -ne 0) {
        Write-Host "Command failed: $Exe $($Arguments -join ' ')" -ForegroundColor Red
        exit $LASTEXITCODE
    }
}

switch ($Target) {
    'up' {
        Invoke-Checked 'docker' @('compose', 'up', '-d', '--build')
        Write-Host ''
        Invoke-Checked 'docker' @('compose', 'ps', '-a')
        Write-Host ''
        Write-Host 'Services can take up to a minute to report healthy. Re-run "docker compose ps" to check.'
    }
    'down' {
        if ($Volumes) {
            Invoke-Checked 'docker' @('compose', 'down', '-v')
        } else {
            Invoke-Checked 'docker' @('compose', 'down')
        }
    }
    'test' {
        if (Get-Command go -ErrorAction SilentlyContinue) {
            Invoke-Checked 'go' @('test', './...')
        } else {
            Write-Host 'Go not found on PATH, running tests in a container.'
            $src = (Get-Location).Path
            Invoke-Checked 'docker' @('run', '--rm', '-v', "${src}:/src", '-w', '/src', 'golang:1.24-alpine', 'go', 'test', './...')
        }
    }
    'logs' {
        $logArgs = @('compose', 'logs', '-f', '--tail=100')
        if ($Service) { $logArgs += $Service }
        & docker @logArgs
    }
}
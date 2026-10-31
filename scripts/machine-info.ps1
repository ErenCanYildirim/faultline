# Writes bench/machine.json: the hardware and Docker limits that benchmark
# results depend on. The load generator copies it into every result file.
# Run it once, and again whenever the hardware or Docker settings change:
#
#   .\scripts\machine-info.ps1
#
# Reads only; changes nothing. Windows only (uses CIM).

$ErrorActionPreference = 'Stop'

function Try-Get([scriptblock]$block, $fallback = $null) {
    try { & $block } catch { $fallback }
}

$cpu = Get-CimInstance Win32_Processor | Select-Object -First 1
$cs = Get-CimInstance Win32_ComputerSystem
$os = Get-CimInstance Win32_OperatingSystem

# The physical disk behind the system drive. Docker Desktop keeps its WSL
# disk image there unless it was moved.
$disk = Try-Get {
    $num = (Get-Partition -DriveLetter $env:SystemDrive.TrimEnd(':')).DiskNumber
    $pd = Get-PhysicalDisk | Where-Object { $_.DeviceId -eq "$num" } | Select-Object -First 1
    "$($pd.FriendlyName) ($($pd.MediaType), $($pd.BusType), $([math]::Round($pd.Size / 1GB)) GB)"
} 'unknown'

# Laptops slow down on battery and in power-saving plans; a benchmark run
# like that is not comparable with one on mains power.
$onBattery = Try-Get {
    $b = Get-CimInstance Win32_Battery | Select-Object -First 1
    if ($null -eq $b) { $false } else { $b.BatteryStatus -ne 2 }
} $null
$powerPlan = Try-Get { ((powercfg /getactivescheme) -replace '.*\((.*)\).*', '$1').Trim() } 'unknown'

# Docker Desktop's VM limits live in .wslconfig (WSL 2 backend). Without the
# file, WSL uses its defaults: half the RAM and all logical processors.
$wslConfigPath = Join-Path $env:USERPROFILE '.wslconfig'
$wslConfig = if (Test-Path $wslConfigPath) { (Get-Content $wslConfigPath -Raw).Trim() } else { $null }

$dockerCpus = Try-Get { [int](docker info --format '{{.NCPU}}') } $null
$dockerMemGiB = Try-Get { [math]::Round([int64](docker info --format '{{.MemTotal}}') / 1GB, 1) } $null

$info = [ordered]@{
    recorded_at        = (Get-Date).ToUniversalTime().ToString('o')
    cpu                = $cpu.Name.Trim()
    cores              = [int]$cpu.NumberOfCores
    logical_processors = [int]$cpu.NumberOfLogicalProcessors
    max_clock_mhz      = [int]$cpu.MaxClockSpeed
    ram_gib            = [math]::Round($cs.TotalPhysicalMemory / 1GB, 1)
    disk               = $disk
    os                 = "$($os.Caption) $($os.Version)"
    on_battery         = $onBattery
    power_plan         = $powerPlan
    docker_cpus        = $dockerCpus
    docker_memory_gib  = $dockerMemGiB
    wslconfig          = $wslConfig
}

$dir = Join-Path $PSScriptRoot '..\bench'
New-Item -ItemType Directory -Force -Path $dir | Out-Null
$path = Join-Path $dir 'machine.json'
$info | ConvertTo-Json | Set-Content -Encoding utf8 $path
Write-Host "wrote $((Resolve-Path $path).Path)"
$info | Format-List
# Failure tests: each one resets the stack to an empty database, starts a
# load, injects one fault at a fixed moment, recovers, then checks for loss
# and draws a chart.
#
#   .\scripts\failure-tests.ps1 -Test kill-worker
#   .\scripts\failure-tests.ps1 -Test all
#   .\scripts\failure-tests.ps1 -Test all -Queue nats
#
# Tests: baseline (no fault, for comparing queues), kill-worker,
# stop-clickhouse, flood, stop-ingest, restart-broker.
# -Queue nats runs the stack on NATS JetStream (docker-compose.nats.yml);
# its files get a -nats suffix so they sit next to the Redpanda ones.
# Output, per test:
#   bench\results\<run-id>-fail-<test>.json   the load test result
#   bench\failure\<test>-losscheck.txt         sent vs stored
#   bench\failure\<test>-*.log                  load generator and service logs
#   bench\charts\fail-<test>.html               the chart, fault moments marked
#
# Each test deletes the database first (docker compose down -v).

param(
    [ValidateSet('baseline', 'kill-worker', 'stop-clickhouse', 'flood', 'stop-ingest', 'restart-broker', 'all')]
    [string]$Test = 'all',
    [ValidateSet('kafka', 'nats')]
    [string]$Queue = 'kafka'
)

$ErrorActionPreference = 'Stop'
Set-Location (Join-Path $PSScriptRoot '..')
New-Item -ItemType Directory -Force -Path bench\failure, bench\charts | Out-Null

# Which broker, and what every name gets appended.
$Broker = if ($Queue -eq 'nats') { 'nats' } else { 'redpanda' }
$Sfx = if ($Queue -eq 'nats') { '-nats' } else { '' }

function Step([string]$msg) { Write-Host ("[{0:HH:mm:ss}] {1}" -f (Get-Date), $msg) -ForegroundColor Cyan }

# Windows PowerShell 5.1 turns every stderr line of a native command whose
# output is captured into an error record, and with ErrorActionPreference
# Stop the first one ends the script, even when the command succeeds.
# docker compose writes its progress ("Container ... Stopping") to stderr.
# So native commands run with Continue, stderr is kept apart from stdout,
# and the exit code decides whether it failed.
function Invoke-Native([string]$exe, [string[]]$argv) {
    $old = $ErrorActionPreference
    $ErrorActionPreference = 'Continue'
    try { $all = & $exe @argv 2>&1 } finally { $ErrorActionPreference = $old }
    $code = $LASTEXITCODE
    $out = @(); $err = @()
    foreach ($line in $all) {
        if ($line -is [System.Management.Automation.ErrorRecord]) { $err += "$line" } else { $out += "$line" }
    }
    [pscustomobject]@{ Code = $code; Out = $out; Err = $err }
}

# Runs docker, returns its stdout, throws with its stderr if it fails.
function Invoke-Docker {
    $r = Invoke-Native 'docker' $args
    if ($r.Code -ne 0) { throw "docker $($args -join ' ') failed (exit $($r.Code)):`n$($r.Err -join "`n")" }
    $r.Out
}

function Build-Tools {
    Step 'building loadgen, losscheck, benchplot'
    foreach ($t in 'loadgen', 'losscheck', 'benchplot') {
        go build -o "bin\$t.exe" "./cmd/$t"
        if ($LASTEXITCODE -ne 0) { throw "go build ./cmd/$t failed" }
    }
}

function Wait-Http([string]$url, [string]$what, [int]$timeoutSec = 120) {
    $deadline = (Get-Date).AddSeconds($timeoutSec)
    while ((Get-Date) -lt $deadline) {
        $code = curl.exe -s -o NUL -w '%{http_code}' $url
        if ($code -eq '200') { return }
        Start-Sleep -Seconds 2
    }
    throw "$what did not become ready at $url within $timeoutSec s"
}

# Empty database, fresh containers, N workers, on the chosen queue.
function Reset-Stack([int]$Workers = 1) {
    Step "resetting the stack (empty database, $Workers worker(s), queue $Queue)"
    # --profile kafka and --remove-orphans: also remove the other broker's
    # containers, left from a run on the other queue.
    Invoke-Docker compose --profile kafka down -v --remove-orphans | Out-Null
    Invoke-Docker compose up -d --scale "worker=$Workers" | Out-Null
    Wait-Http 'http://localhost:8080/readyz' 'ingest'
    Wait-Http 'http://localhost:8123/ping' 'ClickHouse'
    Start-Sleep -Seconds 5 # let the worker join its consumer group
}

# Starts the load generator in the background; returns the process.
function Start-Load([string]$Label, [string]$LoadArgs) {
    $log = "bench\failure\$Label.log"
    $cmdArgs = "$LoadArgs -label $Label -queue $Queue -profile-seconds 0"
    Step "load: loadgen $cmdArgs"
    Start-Process -FilePath 'bin\loadgen.exe' -ArgumentList $cmdArgs -NoNewWindow -PassThru `
        -RedirectStandardOutput $log -RedirectStandardError "$log.err"
}

function Wait-Load($proc, [string]$Label) {
    $proc.WaitForExit()
    Get-Content "bench\failure\$Label.log" | Select-String -Pattern '^(LOSS|WARNING|accounting|sdk |lag |http )' |
        ForEach-Object { Write-Host "    $($_.Line)" }
}

# Sleeps until $sec seconds after $since.
function Wait-Until([datetime]$since, [double]$sec) {
    $left = ($since.AddSeconds($sec) - (Get-Date)).TotalSeconds
    if ($left -gt 0) { Start-Sleep -Milliseconds ([int]($left * 1000)) }
}

function Result-File([string]$Label) {
    $f = Get-ChildItem "bench\results\*-$Label.json" | Sort-Object LastWriteTime | Select-Object -Last 1
    if (-not $f) { throw "no result file for $Label (see bench\failure\$Label.log)" }
    $f.FullName
}

# Seconds from the start of a run to a moment, using the run's own start
# time, so chart markers line up with its timeline exactly.
function Run-Start([string]$resultFile) {
    # Read the raw text: ConvertFrom-Json turns dates into local times in
    # some PowerShell versions and drops the fraction of a second.
    if ((Get-Content $resultFile -Raw) -notmatch '"started":\s*"([^"]+)"') { throw "no start time in $resultFile" }
    [datetime]::Parse($Matches[1], [Globalization.CultureInfo]::InvariantCulture, [Globalization.DateTimeStyles]::AdjustToUniversal)
}
function Run-Offset([string]$resultFile, [datetime]$atUtc) {
    [math]::Round(($atUtc - (Run-Start $resultFile)).TotalSeconds, 1)
}

function Loss-Check([string]$name, [string]$resultFile, [switch]$Optimize) {
    $out = "bench\failure\$name$Sfx-losscheck.txt"
    $a = @('-report', $resultFile)
    if ($Optimize) { $a += '-optimize' }
    # Exit code 1 means loss was found, 2 an error; both are reported, not thrown.
    $r = Invoke-Native '.\bin\losscheck.exe' $a
    ($r.Out + $r.Err) | Tee-Object -FilePath $out | ForEach-Object { Write-Host "    $_" }
    return ($r.Code -eq 0)
}

function Plot([string]$name, [string]$title, [string[]]$runs, [string[]]$panels, [string[]]$marks) {
    $a = @('-o', "bench\charts\fail-$name$Sfx.html", '-title', "$title ($Queue)")
    foreach ($r in $runs) { $a += @('-run', $r) }
    foreach ($p in $panels) { $a += @('-panel', $p) }
    foreach ($m in $marks) { $a += @('-mark', $m) }
    $r = Invoke-Native '.\bin\benchplot.exe' $a
    ($r.Out + $r.Err) | ForEach-Object { Write-Host "    $_" }
    if ($r.Code -ne 0) { throw "benchplot failed for $name" }
}

$summary = @()

# --- 0. Baseline: no fault -----------------------------------------------
# The same load as the fault tests with nothing broken, to compare the two
# queues on equal terms: ingest latency, lag, end-to-end latency, drain.
function Test-Baseline {
    Reset-Stack -Workers 1
    $label = "fail-baseline$Sfx"
    $p = Start-Load $label '-apps 20 -rate 3000 -duration 120s -ramp 5s'
    Wait-Load $p $label
    Get-Content "bench\failure\$label.log" | Select-String -Pattern '^(end-to-end|pipeline|drained|worker |clickhouse )' |
        ForEach-Object { Write-Host "    $($_.Line)" }
    $res = Result-File $label
    $ok = Loss-Check 'baseline' $res
    Plot 'baseline' 'Baseline, no fault' @("$res=$Queue") @('lag', 'acked', 'p99') @()
    $script:summary += [pscustomobject]@{ Test = "baseline$Sfx"; NoLoss = $ok; Result = $res }
}

# --- 1. Kill a worker mid-stream ---------------------------------------
# One of two workers is killed (SIGKILL: no graceful flush, its uncommitted
# batch is redelivered). Redpanda: the workers share six partitions; the
# group notices after the session timeout and moves the dead worker's
# partitions to the survivor. NATS: no partitions; the survivor keeps taking
# new messages at once, and the dead worker's unacknowledged ones come back
# after AckWait (30s).
function Test-KillWorker {
    Reset-Stack -Workers 2
    $label = "fail-kill-worker$Sfx"
    $t0 = Get-Date
    $p = Start-Load $label '-apps 20 -rate 3000 -duration 150s -ramp 5s'
    Wait-Until $t0 30
    $victim = @(Invoke-Docker compose ps -q worker)[0]
    Step "killing worker container $victim"
    Invoke-Docker kill $victim | Out-Null
    $killedAt = (Get-Date).ToUniversalTime()
    Start-Sleep -Seconds 5
    Step ("killed worker state: " + (Invoke-Docker inspect -f '{{.State.Status}} (restarts: {{.RestartCount}})' $victim))
    Wait-Until $t0 100
    Step 'starting the killed worker again'
    Invoke-Docker start $victim | Out-Null
    $restartedAt = (Get-Date).ToUniversalTime()
    Wait-Load $p $label
    Invoke-Docker compose logs worker --no-color | Out-File -Encoding utf8 "bench\failure\kill-worker$Sfx-workers.log"

    $res = Result-File $label
    $ok = Loss-Check 'kill-worker' $res -Optimize
    Plot 'kill-worker' 'Kill a worker mid-stream' @("$res=2 workers, one killed") @('lag', 'acked') `
        @("$(Run-Offset $res $killedAt)=worker killed", "$(Run-Offset $res $restartedAt)=worker restarted")
    if ($Queue -eq 'nats') {
        Write-Host '    expected: no lag spike from a rebalance (there is none): the survivor keeps consuming at once;'
        Write-Host '    the batch the dead worker held comes back after AckWait (30s); LOSS 0.'
    } else {
        Write-Host '    expected: lag rises after the kill until the group moves the partitions (about 45s session timeout),'
        Write-Host '    then drains; LOSS 0.'
    }
    Write-Host '    duplicate rows > 0 only if the killed worker had inserted but not committed; 0 left after merge.'
    $script:summary += [pscustomobject]@{ Test = "kill-worker$Sfx"; NoLoss = $ok; Result = $res }
}

# --- 2. Stop ClickHouse for 60 seconds ----------------------------------
function Test-StopClickHouse {
    Reset-Stack -Workers 1
    $label = "fail-stop-clickhouse$Sfx"
    $t0 = Get-Date
    $p = Start-Load $label '-apps 20 -rate 3000 -duration 120s -ramp 5s -drain-timeout 4m'
    Wait-Until $t0 30
    Step 'stopping ClickHouse'
    Invoke-Docker compose stop clickhouse | Out-Null
    $stoppedAt = (Get-Date).ToUniversalTime()
    Wait-Until $t0 90
    Step 'starting ClickHouse'
    Invoke-Docker compose start clickhouse | Out-Null
    $startedAt = (Get-Date).ToUniversalTime()
    Wait-Load $p $label
    $wlog = "bench\failure\stop-clickhouse$Sfx-worker.log"
    Invoke-Docker compose logs worker --no-color | Out-File -Encoding utf8 $wlog
    $retries = Select-String -Path $wlog -Pattern 'operation failed, retrying'
    Step "worker logged $($retries.Count) insert retries; backoff sequence:"
    $retries | Select-Object -First 8 | ForEach-Object {
        if ($_.Line -match '"retry_in":"([^"]+)"') { Write-Host "    retry in $($Matches[1])" }
    }

    $res = Result-File $label
    $ok = Loss-Check 'stop-clickhouse' $res
    Plot 'stop-clickhouse' 'Stop ClickHouse for 60 seconds' @("$res=ClickHouse stopped 60s") @('lag', 'acked') `
        @("$(Run-Offset $res $stoppedAt)=ClickHouse stopped", "$(Run-Offset $res $startedAt)=ClickHouse started")
    Write-Host '    expected: ingest keeps acknowledging (the broker buffers); lag grows by about 3000/s while'
    Write-Host '    ClickHouse is down; retries back off 200ms, 400ms ... up to 5s; nothing committed; lag drains after; LOSS 0.'
    if ($Queue -eq 'nats') {
        Write-Host '    NATS: the outage outlasts AckWait (30s); KeepAlive holds the batch in hand, so it is not'
        Write-Host '    handed to another worker meanwhile.'
    }
    $script:summary += [pscustomobject]@{ Test = "stop-clickhouse$Sfx"; NoLoss = $ok; Result = $res }
}

# --- 3. Flood one project at 10x its limit -------------------------------
# "noisy" is limited to 50 events/s (deploy/projects.json); it gets 500/s.
# The load-* projects run a normal 2000/s alongside.
function Test-Flood {
    Reset-Stack -Workers 1
    $t0 = Get-Date
    $quiet = Start-Load "fail-flood-quiet$Sfx" '-apps 20 -rate 2000 -duration 90s -ramp 5s'
    Wait-Until $t0 15
    $noisy = Start-Load "fail-flood-noisy$Sfx" '-project-prefix noisy -apps 5 -rate 500 -duration 60s -ramp 0s -drain-timeout 1m'
    Wait-Load $noisy "fail-flood-noisy$Sfx"
    Wait-Load $quiet "fail-flood-quiet$Sfx"

    $q = Result-File "fail-flood-quiet$Sfx"
    $n = Result-File "fail-flood-noisy$Sfx"
    $offset = Run-Offset $q (Run-Start $n)
    $ok1 = Loss-Check 'flood-quiet' $q
    $ok2 = Loss-Check 'flood-noisy' $n
    Plot 'flood' 'Flood one project at 10x its limit' @("$q=normal projects (2000/s)", "$n=noisy project (500/s, limit 50/s)@$offset") `
        @('p99', '429', 'acked') @("$offset=flood starts", "$($offset + 60)=flood ends")
    Write-Host '    expected: noisy gets 429s for everything above 50/s; the normal projects keep their p99 latency'
    Write-Host '    and throughput; LOSS 0 for both (the noisy SDK drops are counted, not lost).'
    $script:summary += [pscustomobject]@{ Test = "flood$Sfx (normal)"; NoLoss = $ok1; Result = $q }
    $script:summary += [pscustomobject]@{ Test = "flood$Sfx (noisy)"; NoLoss = $ok2; Result = $n }
}

# --- 4. Stop ingest ------------------------------------------------------
# 10 apps at 100 events/s each; each SDK buffers at most 1000 events, so
# after about 10s without ingest it starts dropping, and counts the drops.
function Test-StopIngest {
    Reset-Stack -Workers 1
    $label = "fail-stop-ingest$Sfx"
    $t0 = Get-Date
    $p = Start-Load $label '-apps 10 -rate 1000 -duration 90s -ramp 5s'
    Wait-Until $t0 30
    Step 'stopping ingest'
    Invoke-Docker compose stop ingest | Out-Null
    $stoppedAt = (Get-Date).ToUniversalTime()
    Wait-Until $t0 60
    Step 'starting ingest'
    Invoke-Docker compose start ingest | Out-Null
    $startedAt = (Get-Date).ToUniversalTime()
    Wait-Load $p $label

    $res = Result-File $label
    $ok = Loss-Check 'stop-ingest' $res
    Plot 'stop-ingest' 'Stop ingest for 30 seconds' @("$res=ingest stopped 30s") @('generated', 'acked') `
        @("$(Run-Offset $res $stoppedAt)=ingest stopped", "$(Run-Offset $res $startedAt)=ingest started")
    $r = Get-Content $res -Raw | ConvertFrom-Json
    Write-Host ("    SDK dropped {0} events (buffer full), CaptureError p99 {1} ms, max {2} ms" -f `
        $r.sdk.dropped, $r.capture_latency.p99_ms, $r.capture_latency.max_ms)
    Write-Host '    expected: generation continues at 1000/s throughout (the app is not blocked); acknowledged drops'
    Write-Host '    to 0 while ingest is down; the SDK drops what exceeds its buffer and counts it; after ingest returns'
    Write-Host '    (and the SDK breaker cooldown, up to 30s) the buffered events are sent; LOSS 0.'
    $script:summary += [pscustomobject]@{ Test = "stop-ingest$Sfx"; NoLoss = $ok; Result = $res }
}

# --- 5. Restart the broker (optional) ------------------------------------
# Ingest answers 202 only after the broker acknowledged the write. This
# checks that every acknowledged event survives a graceful broker restart
# in the middle of the load. (Single node: there are no replicas, so this
# shows what the write path does, not what replication buys.)
function Test-RestartBroker {
    Reset-Stack -Workers 1
    $label = "fail-restart-$Broker"
    $t0 = Get-Date
    $p = Start-Load $label '-apps 20 -rate 2000 -duration 90s -ramp 5s -drain-timeout 3m'
    Wait-Until $t0 30
    Step "restarting $Broker"
    $restartAt = (Get-Date).ToUniversalTime()
    Invoke-Docker compose restart $Broker | Out-Null
    $backAt = (Get-Date).ToUniversalTime()
    Wait-Load $p $label

    $res = Result-File $label
    $ok = Loss-Check 'restart-broker' $res
    Plot 'restart-broker' "Restart $Broker" @("$res=$Broker restarted") @('acked', 'p99', 'lag') `
        @("$(Run-Offset $res $restartAt)=restart begins", "$(Run-Offset $res $backAt)=$Broker back")
    if ($Queue -eq 'nats') {
        Write-Host '    expected: publishes fail at once while NATS is down (no reconnect buffer), ingest answers 503'
        Write-Host '    and the SDKs retry; every event that got a 202 is stored; LOSS 0.'
    } else {
        Write-Host '    expected: ingest holds requests until Redpanda is back (slow, not rejected); every event that'
        Write-Host '    got a 202 is stored; LOSS 0.'
    }
    $script:summary += [pscustomobject]@{ Test = "restart-$Broker"; NoLoss = $ok; Result = $res }
}

# Every docker compose call in this run uses the NATS override when asked
# for. Set for this script only and put back at the end, since it would
# otherwise stay in your PowerShell session.
$oldComposeFile, $oldSeparator = $env:COMPOSE_FILE, $env:COMPOSE_PATH_SEPARATOR
if ($Queue -eq 'nats') {
    $env:COMPOSE_PATH_SEPARATOR = ';'
    $env:COMPOSE_FILE = 'docker-compose.yml;docker-compose.nats.yml'
} else {
    Remove-Item Env:COMPOSE_FILE, Env:COMPOSE_PATH_SEPARATOR -ErrorAction SilentlyContinue
}
try {
    Build-Tools
    switch ($Test) {
        'baseline' { Test-Baseline }
        'kill-worker' { Test-KillWorker }
        'stop-clickhouse' { Test-StopClickHouse }
        'flood' { Test-Flood }
        'stop-ingest' { Test-StopIngest }
        'restart-broker' { Test-RestartBroker }
        'all' { Test-Baseline; Test-KillWorker; Test-StopClickHouse; Test-Flood; Test-StopIngest; Test-RestartBroker }
    }

    Step 'summary'
    $summary | Format-Table -AutoSize Test, NoLoss, Result
    Step "leaving the stack running on $Queue with one worker; charts are in bench\charts"
    Invoke-Docker compose up -d --scale worker=1 | Out-Null
} finally {
    $env:COMPOSE_FILE, $env:COMPOSE_PATH_SEPARATOR = $oldComposeFile, $oldSeparator
}
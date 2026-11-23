$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

function Invoke-Docker {
    param([Parameter(Mandatory)][string[]] $Arguments)
    & docker @Arguments
    if ($LASTEXITCODE -ne 0) {
        throw "Docker command failed: docker $($Arguments -join ' ')"
    }
}

Invoke-Docker @('build', '-f', 'meridian-producer.Dockerfile', '-t', 'meridian-producer:local', '.')
Invoke-Docker @('build', '-f', 'meridian-batch.Dockerfile', '-t', 'meridian-batch:local', '.')
Invoke-Docker @('build', '-f', 'meridian-stream.Dockerfile', '-t', 'meridian-stream:local', '.')

$producerUser = (& docker run --rm --entrypoint id meridian-producer:local -u).Trim()
if ($LASTEXITCODE -ne 0 -or [int]$producerUser -eq 0) {
    throw 'Producer image must run as a non-root user.'
}
$fixtureCount = (& docker run --rm --entrypoint /bin/sh meridian-producer:local -ec 'grep -c . /opt/kafka/meridian-events.txt').Trim()
if ($LASTEXITCODE -ne 0 -or [int]$fixtureCount -ne 43) {
    throw "Expected 43 tracking fixture events; found $fixtureCount."
}
Write-Output 'PASS: producer image is non-root and contains 43 tracking events.'

$batchUser = (& docker run --rm --entrypoint id meridian-batch:local -u).Trim()
if ($LASTEXITCODE -ne 0 -or [int]$batchUser -eq 0) {
    throw 'Batch image must run as a non-root user.'
}

$streamUser = (& docker run --rm --entrypoint id meridian-stream:local -u).Trim()
if ($LASTEXITCODE -ne 0 -or [int]$streamUser -eq 0) {
    throw 'Streaming image must run as a non-root user.'
}
$streamJars = & docker run --rm --entrypoint /bin/bash meridian-stream:local -c "find /opt/meridian/jars -maxdepth 1 -type f -printf '%f\n'"
if ($LASTEXITCODE -ne 0) { throw 'Could not inspect streaming image dependencies.' }
foreach ($dependency in @('kafka-clients', 'delta-spark', 'iceberg-spark-runtime', 'hadoop-azure')) {
    if (-not ($streamJars -match [regex]::Escape($dependency))) {
        throw "Streaming image is missing dependency: $dependency"
    }
}
Write-Output 'PASS: batch and streaming images are non-root; streaming dependencies are packaged.'

$batchOutput = & cmd.exe /d /c "docker run --rm meridian-batch:local 2>&1"
$batchExitCode = $LASTEXITCODE
if ($batchExitCode -ne 0) {
    $batchOutput | ForEach-Object { Write-Output $_ }
    throw 'Fresh batch execution failed.'
}
$batchOutput | ForEach-Object { Write-Output $_ }
if (($batchOutput -join "`n") -notmatch 'Batch verified: raw=141 latest=133 curated=122 rejected=11') {
    throw 'Fresh batch execution did not produce the expected mini-pack reconciliation.'
}
Write-Output 'PASS: fresh batch run reconciles 141 raw, 133 latest, 122 curated, and 11 rejected rows.'

param(
    [string]$Context = 'docker-desktop',
    [switch]$Adls,
    [ValidatePattern('^[a-z0-9]+$')]
    [string]$RunId
)

$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

$name = 'meridian-stream-live'
$topic = 'shipment-tracking-live'
$checkpoint = '/meridian/checkpoints/live_event_windows'
$warehouse = 'file:///meridian/live-warehouse'
$storageOptions = @()
if ($Adls) {
    $name = 'meridian-stream-adls'
    $topic = 'shipment-tracking-adls'
    $hostName = 'adlsgen2dbs.dfs.core.windows.net'
    $root = "abfss://datasets@$hostName/meridian/streaming"
    $checkpoint = "$root/checkpoints/event_windows"
    $warehouse = "$root/warehouse"
    $storageOptions += "--from-literal=MERIDIAN_ADLS_HOST=$hostName"
}

if ($RunId) {
    $name = "$name-$RunId"
    $topic = "$topic-$RunId"
    $checkpoint = "$checkpoint-$RunId"
    $warehouse = "$warehouse-$RunId"
}

kubectl --context $Context exec deployment/meridian-kafka -- /opt/kafka/bin/kafka-topics.sh --bootstrap-server meridian-kafka:9092 --create --if-not-exists --topic $topic --partitions 1 --replication-factor 1
if ($LASTEXITCODE -ne 0) { throw 'Could not create the live Kafka topic.' }

$config = kubectl --context $Context create configmap $name `
    --from-literal=KAFKA_BOOTSTRAP_SERVERS=meridian-kafka:9092 `
    "--from-literal=KAFKA_TOPIC=$topic" `
    --from-literal=MERIDIAN_STREAM_MODE=continuous `
    "--from-literal=MERIDIAN_CHECKPOINT=$checkpoint" `
    "--from-literal=MERIDIAN_WAREHOUSE=$warehouse" @storageOptions `
    --dry-run=client -o yaml
if ($LASTEXITCODE -ne 0) { throw 'Could not generate the live consumer ConfigMap.' }
$config | kubectl --context $Context apply -f -
if ($LASTEXITCODE -ne 0) { throw 'Could not apply the live consumer ConfigMap.' }

$deployment = (Get-Content -LiteralPath meridian-stream-deployment.yaml) -replace '^([ ]*(?:name|app): )meridian-stream$', ('$1' + $name)
if ($Adls) {
    $deployment = $deployment | ForEach-Object {
        $_
        if ($_ -eq '          envFrom:') {
            '            - secretRef:'
            '                name: meridian-adls-auth'
        }
    }
}
$deployment | kubectl --context $Context apply -f -
if ($LASTEXITCODE -ne 0) { throw 'Could not apply the live consumer deployment.' }

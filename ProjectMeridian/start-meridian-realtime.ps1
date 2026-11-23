$ErrorActionPreference = 'Stop'
Set-Location -LiteralPath $PSScriptRoot

docker compose -f compose.realtime.yaml up -d --build kafka stream
if ($LASTEXITCODE -ne 0) {
    throw 'Could not start the real-time Kafka and Spark services.'
}

Write-Output 'Kafka and the continuous Spark consumer are starting.'
Write-Output 'Check status with: docker compose -f compose.realtime.yaml ps'
Write-Output 'Follow events with: docker compose -f compose.realtime.yaml logs -f stream'
Write-Output 'Publish an event with: .\publish-meridian-event.ps1 -EventJson ''{"event_id":"EVT9999001","shipment_id":"SHP2026008426","hub_id":"HUB06","event_type":"ROUTE_CHECKPOINT","event_time":"2026-11-23T10:00:00Z","ingest_time":"2026-11-23T10:00:01Z","vehicle_id":"VEH-1054","delay_minutes":0}'''

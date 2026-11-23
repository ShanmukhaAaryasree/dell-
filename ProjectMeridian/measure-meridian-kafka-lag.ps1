param(
    [string]$Context = 'docker-desktop',
    [string]$Deployment = 'meridian-stream-adls-recovery',
    [string]$Topic = 'shipment-tracking-adls-recovery'
)

$ErrorActionPreference = 'Stop'
$checkpointReader = @'
import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.Arrays;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

class ReadCheckpoint {
    public static void main(String[] args) throws Exception {
        var env = System.getenv();
        var conf = new Configuration();
        var host = env.get("MERIDIAN_ADLS_HOST");
        if (host != null) {
            conf.set("fs.azure.account.auth.type." + host, "OAuth");
            conf.set("fs.azure.account.oauth.provider.type." + host,
                "org.apache.hadoop.fs.azurebfs.oauth2.ClientCredsTokenProvider");
            conf.set("fs.azure.account.oauth2.client.id." + host, env.get("AZURE_CLIENT_ID"));
            conf.set("fs.azure.account.oauth2.client.secret." + host, env.get("AZURE_CLIENT_SECRET"));
            conf.set("fs.azure.account.oauth2.client.endpoint." + host,
                "https://login.microsoftonline.com/" + env.get("AZURE_TENANT_ID") + "/oauth2/token");
        }
        var checkpoint = new Path(env.getOrDefault("MERIDIAN_CHECKPOINT", "/meridian/checkpoints/event_windows"));
        var fs = checkpoint.getFileSystem(conf);
        long batch = Arrays.stream(fs.listStatus(new Path(checkpoint, "commits")))
            .map(status -> status.getPath().getName())
            .filter(name -> name.matches("[0-9]+"))
            .mapToLong(Long::parseLong)
            .max()
            .orElseThrow(() -> new IllegalStateException("No committed batches found"));
        try (var offsets = new BufferedReader(new InputStreamReader(
                fs.open(new Path(checkpoint, "offsets/" + batch)), StandardCharsets.UTF_8))) {
            offsets.readLine();
            offsets.readLine();
            System.out.println("MERIDIAN_OFFSET " + batch + " " + offsets.readLine());
        }
    }
}
'@
$launchReader = 'set -eu; scratch=$(mktemp -d); trap ''rm -f "$scratch/ReadCheckpoint.java"; rmdir "$scratch"'' EXIT; cat >"$scratch/ReadCheckpoint.java"; java -Xmx160m -cp "/opt/spark/jars/*:/opt/meridian/jars/*" "$scratch/ReadCheckpoint.java"'
$checkpoint = $checkpointReader | kubectl --context $Context exec -i "deployment/$Deployment" -- /bin/bash -c $launchReader
if ($LASTEXITCODE -ne 0) { throw 'Could not read the committed Spark checkpoint.' }
$offsetLine = $checkpoint | Where-Object { $_ -like 'MERIDIAN_OFFSET *' } | Select-Object -First 1
if (!$offsetLine) { throw 'The Spark checkpoint reader returned no offsets.' }

$fields = $offsetLine -split ' ', 3
$committed = ($fields[2] | ConvertFrom-Json).PSObject.Properties[$Topic].Value
if ($null -eq $committed) { throw "The committed batch does not contain $Topic." }

$brokerOffsets = kubectl --context $Context exec deployment/meridian-kafka -- /opt/kafka/bin/kafka-get-offsets.sh --bootstrap-server meridian-kafka:9092 --topic $Topic
if ($LASTEXITCODE -ne 0) { throw 'Could not read Kafka end offsets.' }
$partitions = @($brokerOffsets | Where-Object { $_ -match "^$([regex]::Escape($Topic)):[0-9]+:[0-9]+$" })
if (!$partitions.Count) { throw "Kafka returned no partitions for $Topic." }
foreach ($partition in $partitions) {
    $name, $number, $end = $partition -split ':', 3
    $saved = $committed.PSObject.Properties[$number]
    if ($null -eq $saved -or [long]$end -lt [long]$saved.Value) {
        throw "Kafka and Spark offsets cannot be compared for $name partition $number."
    }
    [pscustomobject]@{
        Topic = $name
        Partition = [int]$number
        CommittedBatch = [long]$fields[1]
        SparkOffset = [long]$saved.Value
        KafkaEndOffset = [long]$end
        Lag = [long]$end - [long]$saved.Value
    }
}

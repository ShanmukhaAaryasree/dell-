$ErrorActionPreference = 'Stop'
$tenantId = [guid](Read-Host 'Directory (tenant) ID')
$clientId = [guid](Read-Host 'Application (client) ID')
$version = kubectl --context docker-desktop -n default get secret meridian-adls-auth -o 'jsonpath={.metadata.resourceVersion}'
if ($LASTEXITCODE -ne 0) { throw 'Could not find the existing Kubernetes Secret.' }

$secureValue = Read-Host 'New client secret VALUE' -AsSecureString
$pointer = [Runtime.InteropServices.Marshal]::SecureStringToBSTR($secureValue)
try {
    $value = [Runtime.InteropServices.Marshal]::PtrToStringBSTR($pointer).Trim()
    $parsedId = [guid]::Empty
    if ([string]::IsNullOrWhiteSpace($value) -or [guid]::TryParse($value, [ref]$parsedId)) {
        throw 'Enter the generated client secret Value. The Secret ID is a GUID.'
    }
    $secret = @{
        apiVersion = 'v1'
        kind = 'Secret'
        metadata = @{
            name = 'meridian-adls-auth'
            namespace = 'default'
            resourceVersion = $version
        }
        type = 'Opaque'
        stringData = @{
            AZURE_TENANT_ID = $tenantId.ToString()
            AZURE_CLIENT_ID = $clientId.ToString()
            AZURE_CLIENT_SECRET = $value
        }
    }
    $secret | ConvertTo-Json -Depth 5 | kubectl --context docker-desktop -n default replace -f -
    if ($LASTEXITCODE -ne 0) { throw 'Kubernetes rejected the Secret update.' }
} finally {
    [Runtime.InteropServices.Marshal]::ZeroFreeBSTR($pointer)
    Remove-Variable secret, value, secureValue -ErrorAction SilentlyContinue
}

param(
    [Parameter(Position = 0)]
    [string] $IdOrMode = "auto",

    [Parameter(Position = 1)]
    [string] $BootstrapHost = "localhost",

    [Parameter(Position = 2)]
    [int] $BootstrapPort = 1099,

    [Parameter(Position = 3)]
    [string] $WorkerHost = "localhost"
)

$jar = Join-Path $PSScriptRoot "worker\target\worker.jar"

if (-not (Test-Path -LiteralPath $jar)) {
    Write-Error "Missing $jar. Build first with: .\.tools\apache-maven-3.10.0\bin\mvn.cmd package"
    exit 1
}

if ($IdOrMode -eq "auto") {
    java -jar $jar auto $BootstrapHost $BootstrapPort $WorkerHost
    exit $LASTEXITCODE
}

$id = 0
if (-not [int]::TryParse($IdOrMode, [ref] $id)) {
    Write-Error "Usage: .\worker.ps1 [auto|<positive id>] [bootstrapHost] [bootstrapPort] [workerHost]"
    exit 1
}

if ($id -lt 1) {
    Write-Error "Worker id must be a positive integer."
    exit 1
}

$port = 5000 + $id
java -jar $jar $id $port $BootstrapHost $BootstrapPort $WorkerHost

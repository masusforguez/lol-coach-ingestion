<#
  Arranca un broker Kafka local en modo KRaft, sin Docker.

  Uso:
    scripts\kafka-start.ps1

  Requiere Java 8 o superior en el PATH y la distribución de Kafka en
  %LOCALAPPDATA%\kafka (ver scripts\kafka-install.ps1). Los scripts .bat de
  Kafka fallan en Windows con "línea demasiado larga", así que aquí se invoca
  la clase principal con classpath wildcard.

  Los datos quedan en data\kafka; la consola del broker en data\kafka.log.
#>
param(
  [string]$KafkaHome = "$env:LOCALAPPDATA\kafka\kafka_2.13-3.7.1",
  [int]$Port = 9092
)

$ErrorActionPreference = "Stop"
$root = Split-Path -Parent $PSScriptRoot
$config = Join-Path $PSScriptRoot "kafka.properties"
$libs = Join-Path $KafkaHome "libs"
$logDir = Join-Path $root "data\kafka"
$logFile = Join-Path $root "data\kafka.log"
$logging = Join-Path $PSScriptRoot "kafka-log4j.properties"

if (-not (Test-Path $libs)) {
  throw "No encuentro Kafka en $KafkaHome. Ejecuta scripts\kafka-install.ps1 primero."
}
if (-not (Get-Command java -ErrorAction SilentlyContinue)) {
  throw "No encuentro java en el PATH. Kafka 3.7 necesita Java 8 o superior."
}

$listening = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if ($listening) {
  throw "El puerto $Port ya está ocupado (PID $($listening[0].OwningProcess)). Kafka parece estar arrancado."
}

New-Item -ItemType Directory -Force -Path $logDir | Out-Null

# KRaft necesita un id de clúster la primera vez, antes de formatear el log dir.
# Los avisos de log4j van por stderr: se silencian para que no paren el script.
$meta = Join-Path $logDir "meta.properties"
if (-not (Test-Path $meta)) {
  Write-Host "Formateando el almacenamiento KRaft en data\kafka..."
  $previous = $ErrorActionPreference
  $ErrorActionPreference = "Continue"
  $clusterId = (& java -cp "$libs\*" kafka.tools.StorageTool random-uuid 2>$null | Out-String).Trim()
  if (-not $clusterId) { throw "No se pudo generar el id de clúster." }
  & java -cp "$libs\*" kafka.tools.StorageTool format -t $clusterId -c $config 2>&1 |
    Where-Object { $_ -notmatch 'log4j|^\s*$' } | Write-Host
  $ErrorActionPreference = $previous
  if (-not (Test-Path $meta)) { throw "El formateo KRaft falló. Revisa el mensaje anterior." }
}

$javaArgs = @(
  "-Dlog4j.configuration=file:$logging",
  "-Dkafka.logs.dir=$($root)\data",
  "-Xmx512M"
)

# Se lanza con cmd /c start para que el broker quede desacoplado de esta consola:
# si se heredara el handle de salida, el script no terminaría nunca.
$cmdLine = 'java ' + (($javaArgs + @("-cp", "`"$libs\*`"", "kafka.Kafka", "`"$config`"")) -join ' ')
$started = Start-Process -FilePath "cmd.exe" `
  -ArgumentList @("/c", "start", "/b", "/min", "cmd", "/c", "$cmdLine > `"$logFile`" 2>&1") `
  -WorkingDirectory $root -WindowStyle Hidden -PassThru

Write-Host "Arrancando Kafka..."
for ($i = 0; $i -lt 60; $i++) {
  Start-Sleep -Milliseconds 500
  if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
    Write-Host "Kafka escuchando en localhost:$Port"
    Write-Host "  datos: data\kafka"
    Write-Host "  log:   data\kafka.log"
    Write-Host "  parar: scripts\kafka-stop.ps1"
    exit 0
  }
}

Write-Host "--- últimas líneas de data\kafka.log ---" -ForegroundColor Yellow
if (Test-Path $logFile) { Get-Content $logFile | Select-Object -Last 20 }
throw "Kafka no escuchó en $Port tras 30 s."

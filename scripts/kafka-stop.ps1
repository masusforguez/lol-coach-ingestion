<#
  Para el broker Kafka local arrancado con scripts\kafka-start.ps1.

  Uso:
    scripts\kafka-stop.ps1

  Busca el proceso java que escucha en el puerto y lo termina. Los datos de
  data\kafka se conservan: al volver a arrancar, los topics y los offsets
  siguen ahí.
#>
param([int]$Port = 9092)

$ErrorActionPreference = "Stop"

$conn = Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue
if (-not $conn) {
  Write-Host "No hay nada escuchando en el puerto ${Port}: Kafka no está arrancado."
  exit 0
}

$pids = $conn | Select-Object -ExpandProperty OwningProcess -Unique
foreach ($processId in $pids) {
  Stop-Process -Id $processId -Force -ErrorAction SilentlyContinue
  Write-Host "Proceso $processId detenido."
}

Start-Sleep -Milliseconds 500
if (Get-NetTCPConnection -LocalPort $Port -State Listen -ErrorAction SilentlyContinue) {
  throw "El puerto $Port sigue ocupado."
}
Write-Host "Kafka parado. Los datos de data\kafka se conservan."

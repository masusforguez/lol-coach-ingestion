<#
  Descarga y descomprime Kafka 3.7.1 (la última versión que funciona con
  Java 8) en %LOCALAPPDATA%\kafka. Solo hay que ejecutarlo una vez.

  Uso:
    scripts\kafka-install.ps1
    scripts\kafka-install.ps1 -Version 3.7.1
#>
param(
  [string]$Version = "3.7.1",
  [string]$Scala = "2.13",
  [string]$Destination = "$env:LOCALAPPDATA\kafka"
)

$ErrorActionPreference = "Stop"
$ProgressPreference = "SilentlyContinue"

$name = "kafka_$Scala-$Version"
$home = Join-Path $Destination $name

if (Test-Path (Join-Path $home "libs")) {
  Write-Host "Kafka $Version ya está en $home"
  exit 0
}

$java = Get-Command java -ErrorAction SilentlyContinue
if (-not $java) {
  throw "No encuentro java en el PATH. Kafka 3.7 necesita Java 8 o superior."
}

New-Item -ItemType Directory -Force -Path $Destination | Out-Null
$archive = Join-Path $Destination "$name.tgz"
$url = "https://archive.apache.org/dist/kafka/$Version/$name.tgz"

Write-Host "Descargando $url"
Invoke-WebRequest -Uri $url -OutFile $archive -TimeoutSec 600

Write-Host "Descomprimiendo en $Destination"
tar -xzf $archive -C $Destination
Remove-Item $archive -Force

if (-not (Test-Path (Join-Path $home "libs"))) {
  throw "La extracción no produjo $home\libs"
}

Write-Host "Kafka $Version instalado en $home"
Write-Host "Arranca el broker con: scripts\kafka-start.ps1"

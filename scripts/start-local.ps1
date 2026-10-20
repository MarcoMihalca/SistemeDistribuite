# Script PowerShell pentru pornirea locala a celor 3 noduri pe porturi diferite (Windows)
$ErrorActionPreference = "Stop"

if (-not (Test-Path "target/classes")) {
    Write-Host "Compilare clase Java in target/classes..."
    New-Item -ItemType Directory -Force -Path "target/classes" | Out-Null
    javac -d target/classes (Get-ChildItem -Recurse -Filter *.java src/main/java | Select-Object -ExpandProperty FullName)
}

java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator
java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2
java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3

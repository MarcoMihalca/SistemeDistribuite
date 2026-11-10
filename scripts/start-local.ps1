# Script PowerShell pentru pornirea locala a celor 3 noduri pe porturi diferite (Windows)
$ErrorActionPreference = "Stop"

# Opreste eventualele procese mai vechi ramase pe porturile 7001, 7002, 7003
foreach ($p in @(7001, 7002, 7003)) {
    $conns = Get-NetTCPConnection -LocalPort $p -State Listen -ErrorAction SilentlyContinue
    foreach ($c in $conns) {
        if ($c.OwningProcess -gt 0) {
            Write-Host "Oprire proces vechi de pe portul $p (PID=$($c.OwningProcess))..."
            Stop-Process -Id $c.OwningProcess -Force -ErrorAction SilentlyContinue
        }
    }
}

Write-Host "Compilare clase Java (--release 8) in target/classes..."
New-Item -ItemType Directory -Force -Path "target/classes" | Out-Null
javac --release 8 -Xlint:-options -d target/classes (Get-ChildItem -Recurse -Filter *.java src/main/java | Select-Object -ExpandProperty FullName)

Write-Host "Pornire nod n1 (Coordonator, port 7001)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator"

Write-Host "Pornire nod n2 (Stocare, port 7002)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2"

Write-Host "Pornire nod n3 (Stocare, port 7003)..."
Start-Process powershell -ArgumentList "-NoExit", "-Command", "java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3"

Write-Host "Cele 3 noduri au fost pornite in ferestre separate!"
Write-Host "Pentru a porni clientul, ruleaza: java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001"

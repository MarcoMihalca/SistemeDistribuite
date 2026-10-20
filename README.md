# Sistem Distribuit de Stocare a Fișierelor

Proiect realizat in intregime de **Marco Mihalca**.

## Compilare

Pentru a compila proiectul cu Maven din rădăcina repository-ului:

```bash
mvn package
```

## Pornirea Proceselor

Fiecare componenta ruleaza ca si un nod separat, deci vom avea nevoie de cate un terminal pentru fiecare nod.

### 1. Pornirea nodurilor de stocare (3 terminale separate)

**Terminal 1 (Nodul `n1` — Coordonator + Nod de stocare):**
```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator
```

**Terminal 2 (Nodul `n2` — Nod de stocare):**
```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2
```

**Terminal 3 (Nodul `n3` — Nod de stocare):**
```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3
```

*(Optional se poate folosi `./scripts/start-local.sh` pe Linux/MacOS sau `.\scripts\start-local.ps1` pe Windows, pentru a porni automat cele 3 noduri.)*

### 2. Pornirea clientului (Terminalul 4)

```bash
java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001
```

La pornire, fiecare nod își creează directorul de date dacă acesta nu există și afișează configurația:
```text
[n2] pornit: port 7002, director data/n2, rol: nod de stocare
```
Iar clientul afișează coordonatorul configurat și comenzile disponibile:
```text
[client] coordonator configurat: localhost:7001; comenzi: put, get, ls, rm, nodes, exit
```
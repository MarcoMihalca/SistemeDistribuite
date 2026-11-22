# Sistem Distribuit de Stocare a Fișierelor

Proiect realizat în întregime de **Marco Mihalca**.

Sistem distribuit de stocare a fișierelor dezvoltat în **Java 8+** (`maven.compiler.release = 8`), fără dependențe externe, cu **împărțire adaptivă pe bucăți (HDFS Hybrid)** (`<= 2 MB` fără split, `2 MB / 16 MB / 64 MB / 128 MB`, limită `4 GB`), transfer paralel prin TCP cu `FileChannel`, verificare de integritate SHA-256 (per bucată și global), coordonare metadate (*Least-Bytes-Used Striping*) și descoperire/heartbeat hibridă (UDP Multicast + TCP Unicast).

---

## Compilare

Pentru a compila proiectul cu Maven din rădăcina repository-ului:

```bash
mvn package
```

*(Sau `mvn -q package` pentru compilare rapidă fără output detaliat. Pe Windows, scriptul `.\scripts\start-local.ps1` compilează automat sursele cu `javac --release 8` în `target/classes`.)*

---

## Pornirea nodurilor de stocare

### Opțiunea 1 — Rapid pe Windows (PowerShell)

```powershell
.\scripts\start-local.ps1
```

Scriptul oprește eventualele procese mai vechi care ascultă pe porturile `7001–7003`, compilează clasele Java în `target/classes` și deschide 3 ferestre PowerShell separate pentru:
* `n1` — port `7001`, director `data/n1`, rol `--coordinator`
* `n2` — port `7002`, director `data/n2`
* `n3` — port `7003`, director `data/n3`

### Opțiunea 2 — Rapid pe Linux / macOS

```bash
mvn -q package && ./scripts/start-local.sh
```

*(Notă: `scripts/start-local.sh` rulează automat `mvn -q package` doar dacă lipsește `target/classes`; după modificări în cod, recompilați explicit cu `mvn -q package`.)*

### Opțiunea 3 — Manual (terminale separate)

Sintaxa generală pentru un nod (`StorageNode`):

```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id <id> --port <port> --dir <director> [--coordinator] [--host <host>]
```

*(Parametrul `--host` este opțional și are valoarea implicită `localhost`.)*

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

Puteți porni oricând un nod suplimentar la rulare (de exemplu `n4` pe portul `7004`), iar acesta va fi descoperit automat prin heartbeat și va primi prioritar bucăți noi până la echilibrare:
```bash
java -cp target/classes ro.ubm.sd.node.StorageNode --id n4 --port 7004 --dir data/n4
```

La pornire, fiecare nod își creează directorul de date dacă nu există și afișează configurația:
```text
[n2] pornit (PID=12345): port 7002, director data/n2, rol: nod de stocare (interval heartbeat: 5s, prag CAZUT: 5 intervale)
```

---

## Pornirea clientului

### Mod interactiv (REPL)

Necesită specificarea adresei coordonatorului prin `--coordinator <host:port>`:

```bash
java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001
```

La pornire, clientul afișează coordonatorul configurat și comenzile disponibile:
```text
[client] coordonator configurat: localhost:7001; comenzi: put, get, ls, info, rm, nodes, freeze <port>, unfreeze <port>, help, exit
```

### Mod direct (o singură comandă din linia de comandă)

```bash
java -cp target/classes ro.ubm.sd.client.Client [--coordinator <host:port>] <comanda> [argumente]
```

*(În modul direct, `--coordinator` este opțional și are valoarea implicită `localhost:7001`.)*

Exemple:
```bash
java -cp target/classes ro.ubm.sd.client.Client nodes
java -cp target/classes ro.ubm.sd.client.Client put raport.pdf
java -cp target/classes ro.ubm.sd.client.Client info raport.pdf
java -cp target/classes ro.ubm.sd.client.Client get raport.pdf ./out
java -cp target/classes ro.ubm.sd.client.Client put localhost:7002 test.txt
java -cp target/classes ro.ubm.sd.client.Client get localhost:7002 test.txt ./out
```

---

## Comenzi disponibile în `Client`

| Comandă | Descriere |
| :--- | :--- |
| `put <fisier_local>` | Încărcare distribuită pe bucăți (`ALLOCATE` $\rightarrow$ `PUT` paralel $\rightarrow$ `COMMIT`), cu praguri adaptive (`<= 2 MB` întreg, apoi `2 / 16 / 64 / 128 MB`, max `4 GB`), verificare SHA-256 per bucată + global și rollback automat la eșec (numele fișierului nu poate conține spații). |
| `put <nod\|host:port> <fisier_local>` | Încărcare directă pe un nod specificat prin ID (ex. `n2`) sau adresă `host:port` (ex. `localhost:7002`). |
| `get <nume_sau_id> [dir_dest]` | Descărcare paralelă pe bucăți (`LOCATE` $\rightarrow$ `GET` paralel în `FileChannel`) în `dir_dest` (implicit `.`), cu verificare SHA-256 per bucată și pe fișierul reasamblat. |
| `get <nod\|host:port> <nume_fisier> [dir_dest]` | Descărcare directă de pe un nod în `dir_dest` (implicit `.`), cu verificare SHA-256. |
| `ls` | Listează fișierele înregistrate în metadatele coordonatorului (`UUID`, nume, dimensiune, nr./dimensiune bucăți, SHA-256, noduri). |
| `info <nume_sau_id>` *(alias: `stat`)* | Afișează metadatele complete și harta detaliată a bucăților (`#`, `CHUNK ID`, `OFFSET`, `DIMENSIUNE`, `SHA-256`, `NODURI`). |
| `rm <nume_sau_id>` | Șterge fișierul din metadatele coordonatorului și elimină toate bucățile asociate de pe nodurile de stocare (`DELETE` + `REMOVE_REPLICA`). |
| `nodes` | Afișează tabela nodurilor din cluster (`ACTIV`, `SUSPECT`, `CAZUT`, intervale ratate, ultimul heartbeat, PID, rol). |
| `freeze <port\|host:port>` | Oprește temporar emiterea heartbeat-ului pe nodul țintă pentru testarea detecției defectelor (`3` intervale / `15s` $\rightarrow$ `SUSPECT`, `5` intervale / `25s` $\rightarrow$ `CAZUT` + oprire forțată). |
| `unfreeze <port\|host:port>` *(alias: `resume`)* | Reia emiterea heartbeat-ului pe nodul țintă (trebuie rulată înainte de pragul de `25s`). |
| `help` | Afișează lista comenzilor disponibile. |
| `exit` *(alias: `quit`)* | Închide clientul interactiv. |

---

## Comenzi în consola locală a unui nod (`StorageNode`)

În terminalul fiecărui nod pornit sunt disponibile următoarele comenzi interactive:

| Comandă | Descriere |
| :--- | :--- |
| `nodes` / `status` | Afișează tabela locală cu nodurile cunoscute din cluster, starea și intervalele ratate. |
| `freeze` / `pause` | Oprește manual emiterea heartbeat-ului pentru nodul curent (pentru testare). |
| `resume` / `unfreeze` | Reia imediat emiterea heartbeat-ului pentru nodul curent. |
| `exit` / `quit` | Oprește curat nodul curent. |

---

## Documentație suplimentară

* [docs/HDFS_specs.md](docs/HDFS_specs.md) — specificația împărțirii adaptive pe bucăți (HDFS Hybrid), plasarea *Least-Bytes-Used* și tratarea eșecurilor.
* [docs/Heartbeat_specs.md](docs/Heartbeat_specs.md) — descoperirea nodurilor (UDP Multicast + TCP Unicast), intervalele de Heartbeat și auto-kill.
* [docs/Architecture_specs.md](docs/Architecture_specs.md) — arhitectura control/date, încadrarea TCP, scrierea atomică și modelul de fire de execuție.
* [docs/protocol.md](docs/protocol.md) — protocolul complet de comunicație TCP și mesajele de control.
* [docs/cerinte.md](docs/cerinte.md) — cerințe funcționale, model de date, obiective măsurabile și scenariul de demonstrație.

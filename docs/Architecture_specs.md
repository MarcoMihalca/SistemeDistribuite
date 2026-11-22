# Specificație de Arhitectură și Comunicație TCP (`sd-storage`)

Acest document centralizează arhitectura generală a sistemului distribuit `sd-storage`, încadrarea mesajelor TCP, modelul de fire de execuție și stadiul curent față de planul etapelor.

---

## 1. Separarea Planului de Control de Planul de Date

Sistemul urmează modelul arhitectural HDFS pentru a evita transformarea coordonatorului în gât de sticlă (*bottleneck*) I/O:

* **Planul de control (`Client` $\leftrightarrow$ `Coordonator`):** transportă exclusiv comenzi ușoare și metadate (`ALLOCATE`, `COMMIT`, `LOCATE`, `LIST`, `DELETE`, `NODES_STATUS`).
* **Planul de date (`Client` $\leftrightarrow$ `StorageNode`-uri):** octeții reali ai fișierelor și bucăților circulă **direct punct-la-punct peste TCP** (`PUT`, `GET`) în blocuri de `64 KB`, în paralel pe maximum `4` thread-uri.

```text
  Client ──(1) ALLOCATE / LOCATE / COMMIT / LIST / DELETE──> Coordonator (StorageNode --coordinator)
    │                                                              │
    └──────(2) PUT / GET direct pe bucati (TCP, 64 KB, paralel)────┴──> StorageNode-uri (n1, n2, n3, ...)
```

---

## 2. Încadrarea mesajelor TCP și siguranța pe disc

Implementată în [Protocol.java](../src/main/java/ro/ubm/sd/common/Protocol.java):

1. **Antet UTF-8 pe o singură linie:**
   * Fiecare mesaj începe cu un antet text terminat cu `\n` (limitat la maximum `MAX_HEADER_BYTES = 16 KB`).
   * `Protocol.readHeaderLine(InputStream)` citește **octet cu octet** până la `\n` fără `BufferedReader`, garantând că niciun octet din payload-ul binar imediat următor nu este pre-încărcat greșit.
2. **Transfer binar în flux (`64 KB`):**
   * `Protocol.BUFFER_SIZE = 65_536` octeți (`64 KB`).
   * Nici clientul, nici nodurile nu încarcă fișierul întreg în memorie RAM (suportă fișiere până la `MAX_FILE_SIZE = 4 GB`).
   * Pentru bucăți (`chunks`), clientul folosește `FileChannel` (`sendFileRange` / `receiveBytesToChannel`) pentru citiri și scrieri poziționale directe la `offset`.
3. **Scriere atomică cu fișiere `.part`:**
   * Atât nodurile la `PUT`, cât și clientul la `GET`, scriu datele recepționate într-un fișier temporar `<nume>.part`.
   * Redenumirea la ținta finală se face prin `Protocol.moveAtomically` (`StandardCopyOption.ATOMIC_MOVE` + `REPLACE_EXISTING`, cu fallback la `REPLACE_EXISTING`) **abia după** recepționarea integrală și validarea checksum-ului **SHA-256**.
   * La orice eroare de rețea sau checksum invalid, `.part` este șters.
4. **Sanitizarea numelor (`Protocol.sanitizeFileName`):**
   * Respinge nume goale, `.`, `..`, separatori de cale (`/` sau `\`) și spații albe, prevenind atacurile de tip *path traversal* și erorile de parsare în antete.

---

## 3. Modelul de fire de execuție (*Threading*)

### Pe fiecare `StorageNode`
* **`tcp-server-<id>`** (thread daemon): rulează `ServerSocket.accept()` și deleagă fiecare conexiune.
* **`tcp-handler-<id>`** (`Executors.newCachedThreadPool`, daemon): procesează conexiunile TCP cu timeout de **`60_000 ms`** (`60s`).
* **`heartbeat-scheduler`** (`Executors.newScheduledThreadPool(2)`, daemon):
  * Task 1 (`0s`, apoi din `5s` în `5s`): `emitHeartbeatCycle()`.
  * Task 2 (`5s`, apoi din `5s` în `5s`): `evaluateNodesIntervalCycle()`.
* **`udp-multicast-<id>`** (thread daemon): recepționează datagramele UDP pe `230.0.0.1:6999`.
* **Consola locală (`main`):** citește comenzi interactive din `stdin` (`nodes`, `freeze`, `resume`, `exit`).

### Pe `Client`
* **Conexiuni de control:** timeout conectare `2000 ms`, timeout date `60_000 ms`.
* **Pool paralel per transfer:** `Executors.newFixedThreadPool(Math.min(chunkCount, 4))` pentru upload și download paralel pe bucăți.

---

## 4. Tehnologii și compatibilitate

* **Limbaj & Runtime:** **Java 8+** (`<maven.compiler.release>8</maven.compiler.release>` în [pom.xml](../pom.xml)), fără biblioteci externe (exclusiv JDK standard: `java.net`, `java.nio.channels.FileChannel`, `java.security.MessageDigest`, `java.util.concurrent`).
* **Sincronizare:** tabelele `fileMetadataTable` și `clusterNodes` folosesc `ConcurrentHashMap`; operațiile de commit/delete de metadate sunt protejate de `metadataLock`; stările nodurilor din `NodeInfo` sunt `synchronized`.

---

## 5. Stadiul implementării față de planul etapelor

| Etapă | Funcționalitate | Status |
| :--- | :--- | :--- |
| **Etapa 1** | Structură Maven, procese separate (`StorageNode`, `Client`), specificații `cerinte.md` | **Complet** |
| **Etapa 2** | Transfer TCP `64 KB`, SHA-256, scriere atomică `.part`, coordonare metadate | **Complet** |
| **Etapa 3** | Descoperire hibridă UDP Multicast + TCP Unicast, Heartbeat `5s` (`ACTIV`/`SUSPECT`/`CAZUT`), auto-kill & `freeze` | **Complet** |
| **Extensie HDFS** | Împărțire adaptivă pe bucăți (`<=2MB`, `2/16/64/128 MB`, max `4 GB`), *Least-Bytes-Used Striping*, upload/download paralel `FileChannel`, rollback la eșec, `info` | **Complet** (`HDFS-Integration`) |
| **Etapa 4** | Benchmark de scalabilitate (10 fișiere × `100 MB` pe 2 vs 3/4 noduri) | **Planificat** |
| **Etapa 5** | Alegerea dinamică a coordonatorului prin algoritmul **Bully** (`ELECTION`/`ALIVE`/`COORDINATOR`) | **Planificat** |
| **Etapa 6** | Replicare în lanț $R = 2$ (`REPLICATE`), fișiere `<chunkId>.meta` și reconstrucție `REPORT_FILES` (RMI/gRPC) | **Planificat** |
| **Etapa 7** | Demonstrația completă cap-coadă (scenariul din `cerinte.md`) | **Planificat** |

# Specificație Tehnică: Stocare Adaptivă pe Bucăți (*HDFS Hybrid Chunking*)

Acest document descrie arhitectura, algoritmii de împărțire și plasare, protocolul TCP și mecanismele de protecție la defecte pentru stocarea distribuită pe bucăți (*chunks*) în sistemul `sd-storage`.

---

## 1. Motivație: De ce HDFS Hybrid și nu bloc fix?

În HDFS clasic, toate fișierele sunt împărțite după o dimensiune fixă mare (ex. `64 MB` sau `128 MB`), ceea ce înseamnă că fișierele medii (ex. `5–20 MB`) ajung integral pe un singur nod, fără paralelism. Pe de altă parte, o dimensiune fixă mică (ex. `1 MB`) creează sute sau mii de conexiuni TCP și intrări de metadate pentru un fișier de `1 GB`.

Abordarea **HDFS Hybrid** din [ChunkPolicy.java](../src/main/java/ro/ubm/sd/common/ChunkPolicy.java) folosește **praguri adaptive puteri ale lui 2**, aliniate cu buffer-ul TCP de **`64 KB`** ([Protocol.java](../src/main/java/ro/ubm/sd/common/Protocol.java)), plus o **ocolire pentru fișiere mici** (*small-file bypass*).

---

## 2. Pragurile adaptive și dimensiunea maximă

Limita maximă per fișier (`Protocol.MAX_FILE_SIZE`) este **`4 GB`** (`4L * 1024 * 1024 * 1024` octeți).

| Dimensiune fișier | Dimensiune bucată | Nr. bucăți rezultat | Motivație |
| :--- | :--- | :---: | :--- |
| `0 B` – `2 MB` | **Fără împărțire** (fișier întreg) | `1` | Evită overhead-ul de handshake TCP și metadate multiple pentru fișiere mici (inclusiv `0 B`). |
| `> 2 MB` – `16 MB` | **`2 MB`** | `2 – 8` | Distribuie vizibil pe mai multe noduri chiar și fișiere de test/demo de `5–10 MB`. |
| `> 16 MB` – `128 MB` | **`16 MB`** | `2 – 8` | Optim pentru fișiere de ~`100 MB` (ex. `100 MB` $\rightarrow$ `7` bucăți, benchmark Etapa 4). |
| `> 128 MB` – `1 GB` | **`64 MB`** | `3 – 16` | Dimensiune clasică de bloc HDFS; echilibru optim între throughput și compactitatea metadatelor. |
| `> 1 GB` – `4 GB` | **`128 MB`** | `9 – 32` | Default HDFS modern; previne explozia de socket-uri și metadate pe fișiere multi-GB. |

### Proprietăți cheie
* **Fan-out controlat (`2–16` bucăți până la `1 GB`):** fiecare fișier obține paralelism multi-nod fără a deschide sute de conexiuni concurente.
* **Aliniere exactă:** toate dimensiunile (`2 MB`, `16 MB`, `64 MB`, `128 MB`) sunt multipli exacți ai ferestrei de transfer de `64 KB`.

---

## 3. Modelul de date și stocarea pe disc

* **`ChunkMetadata`** ([ChunkMetadata.java](../src/main/java/ro/ubm/sd/common/ChunkMetadata.java)): stochează `index` (`0..N-1`), `chunkId` (`String`), `offset` (`long`), `size` (`long`), `checksum` (SHA-256 hex) și `replicaNodes` (`List<String>` imutabilă).
* **`FileMetadata`** ([FileMetadata.java](../src/main/java/ro/ubm/sd/common/FileMetadata.java)): păstrează `id` (`UUID`), `name`, `size`, `checksum` (SHA-256 global), `chunkSize`, lista ordonată `List<ChunkMetadata> chunks` și reuniunea sortată `replicaNodes` a tuturor nodurilor care dețin cel puțin o bucată a fișierului.
* **Identificarea pe disc:** fiecare bucată este salvată în directorul nodului asignat sub numele unic:
  `<fileUUID>.chunk.<index>` (ex. `data/n2/3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90.chunk.0`).
* **Compatibilitate cu Etapa 2 și Etapa 6:**
  * Handlerele `PUT`, `GET` și `REMOVE_REPLICA` din [StorageNode.java](../src/main/java/ro/ubm/sd/node/StorageNode.java) procesează bucățile fără modificări speciale de format.
  * Comenzile directe către un singur nod (`put localhost:7002 ...`, `get localhost:7002 ...`) rămân 100% compatibile.
  * Structura `List<String> replicaNodes` pe fiecare `ChunkMetadata` este pregătită nativ pentru replicarea $R = 2$ și reconstrucția metadatelor (Etapa 6).

---

## 4. Algoritmul de plasare (*Least-Bytes-Used Striping*)

La primirea cererii `ALLOCATE <nume> <dimensiune> <sha256>` în `StorageNode.handleAllocate`:

1. **Selecția nodurilor eligibile:** sunt luate în calcul toate nodurile din `clusterNodes` aflate în starea **`ACTIV`** (sortate după ID), **inclusiv coordonatorul `n1`**, astfel încât un cluster de 3 noduri să beneficieze de *striping* în 3 căi. Nodurile `SUSPECT` și `CAZUT` sunt excluse.
2. **Calculul încărcării curente:** pentru fiecare nod activ, coordonatorul însumează dimensiunile tuturor bucăților existente în `fileMetadataTable` care au nodul respectiv în `replicaNodes`.
3. **Alocare greedy per bucată:** pentru fiecare bucată `i = 0 .. chunkCount - 1`:
   * Se alege nodul activ cu **cei mai puțini octeți alocați** (la egalitate, departajarea pornește ciclic de la `(tieStart + i) % activeNodes.size()`, unde `tieStart` crește la fiecare `ALLOCATE`).
   * Încărcarea proiectată a nodului ales este incrementată imediat cu `chunkLen`, astfel încât bucățile aceluiași fișier să se distribuie uniform pe noduri diferite.
4. **Auto-echilibrare la adăugarea de noduri:** pe un cluster gol (`n1, n2, n3`), bucățile alternează `n1 -> n2 -> n3 -> n1...`. Dacă se adaugă un nod nou `n4` la rulare (`0` octeți alocați), acesta primește automat prioritate la următoarele alocări până când încărcarea se echilibrează.

---

## 5. Streaming *Zero-Temp-Copy* și paralelism

* **Hashing într-o singură trecere (`ChecksumUtil.sha256WithChunks`):** la `put`, clientul citește fișierul local o singură dată secvențial (în blocuri de `64 KB`) și calculează simultan atât **SHA-256 global**, cât și **SHA-256 pentru fiecare bucată**. Nu se scriu fișiere intermediare pe disc.
* **Upload paralel (`Client.uploadCoordinated`):**
  * Folosește un pool limitat de maximum **`MAX_PARALLEL_CHUNKS = 4`** thread-uri.
  * Fiecare worker deschide o conexiune TCP către nodul alocat bucății și transmite direct intervalul `[offset, offset + size)` folosind `FileChannel` (`Protocol.sendFileRange`), fără a încărca bucata în RAM.
* **Download paralel (`Client.downloadCoordinated`):**
  * Clientul deschide un singur fișier temporar `<nume>.part` prin `FileChannel`.
  * Maximum `4` thread-uri descarcă bucățile în paralel și le scriu direct la `offset`-ul exact prin `FileChannel.write(ByteBuffer, pos)` (`Protocol.receiveBytesToChannel`), operație sigură pentru intervale disjuncte — fără spațiu suplimentar pe disc și fără pas separat de concatenare.
  * După închiderea `FileChannel`, clientul verifică dimensiunea totală și **SHA-256 global** pe `.part` înainte de `Protocol.moveAtomically`.

---

## 6. Fluxul de protocol și comanda `info`

1. **`ALLOCATE` $\rightarrow$ `ALLOC_PLAN`:**
   * Coordonatorul răspunde cu `ALLOC_PLAN <fileId> <chunkSize> <chunkCount>` urmat de `chunkCount` linii:
     `CHUNK <i> <chunkId> <offset> <chunkLen> <nodeId>@<host>:<port>`
2. **Transfer bucăți (`PUT`):** clientul trimite fiecare bucată direct nodului desemnat (`PUT <chunkId> <chunkLen> <chunkSha256>`).
3. **Confirmare (`COMMIT`):** doar dacă toate bucățile au fost stocate cu `STORED`, clientul trimite:
   `COMMIT <fileId> <name> <size> <fullSha256> <chunkSize> <chunkCount>` urmat de `chunkCount` linii `CHUNK <i> <chunkId> <offset> <chunkLen> <chunkSha256> <nodeIds_csv>`.
4. **Localizare (`LOCATE`) și inspecție (`info <nume_sau_id>`):**
   * Coordonatorul răspunde cu `LOCATIONS <uuid> <name> <size> <fullSha256> <chunkSize> <chunkCount>` urmat de `chunkCount` linii `CHUNK ... <endpoints_csv>`.
   * Comanda nouă `info <nume_sau_id>` (alias `stat`) din `Client` afișează distribuția completă pe bucăți (index, `chunkId`, `offset`, dimensiune, `SHA-256`, noduri).
5. **Listare (`ls` / `LIST`) și Ștergere (`rm` / `DELETE`):**
   * `LIST` include numărul de bucăți și dimensiunea maximă de bloc (`<chunkCount> <chunkSize>`).
   * `DELETE` șterge intrarea din `fileMetadataTable` și trimite `REMOVE_REPLICA <chunkId>` către nodurile care dețin bucățile.

---

## 7. Scenarii limită și tratarea eșecurilor (*Edge Cases*)

| Scenariu / Defect | Comportament și protecție |
| :--- | :--- |
| **O bucată eșuează la `put`** *(nod picat, timeout `60s`, SHA-256 greșit)* | Workerii setează flag-ul de eșec; clientul **nu** trimite `COMMIT` (fișierul rămâne invizibil în `ls`/`get`) și rulează curățare *best-effort* (`REMOVE_REPLICA <chunkId>`) pentru bucățile deja încărcate. |
| **Întrerupere TCP în timpul `PUT` pe nod** | Nodul scrie în `<chunkId>.part` și execută `ATOMIC_MOVE` abia după confirmarea SHA-256; în caz de EOF/eroare, fișierul `.part` este șters. |
| **Date corupte la `get`** | Dublă validare: SHA-256 per bucată la recepția în canal + SHA-256 pe întregul fișier `.part` înainte de `ATOMIC_MOVE`. Dacă o replică este coruptă, se încearcă următorul endpoint din listă; dacă niciuna nu este validă, `.part` este șters. |
| **Nod cu o bucată `CAZUT` la `get`** | Cât timp $R = 1$, coordonatorul returnează `ERROR CHUNK_OFFLINE <index>` înainte de transfer; cu replicarea $R = 2$ (Etapa 6), clientul face fallback automat pe a doua replică. |
| **Nod `SUSPECT` sau `CAZUT` la `put`** | Este exclus din `handleAllocate`. Dacă rămâne un singur nod `ACTIV`, toate bucățile merg pe acel nod fără blocaj. Dacă nu există niciun nod `ACTIV`, alocarea răspunde `ERROR NO_ACTIVE_NODES`. |
| **Fișier duplicat sau nume invalid** | Respins la `ALLOCATE` (`ERROR FILE_ALREADY_EXISTS` / `ERROR INVALID_FILENAME`) și revalidat sincronizat sub `metadataLock` la `COMMIT`. |
| **Fișiere mici (`<= 2 MB`, inclusiv `0 B`)** | Nu sunt împărțite (`1` bucată, `1` conexiune TCP). Pentru `0 B`, `chunkSize = 0`, `chunkCount = 1`, cu SHA-256 valid al fluxului gol. |

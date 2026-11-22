# Protocol de Comunicație (`sd-storage`)

## 1. Încadrarea mesajelor TCP

Toate mesajele peste **TCP** folosesc același format:
1. **Un antet text UTF-8 pe o singură linie**, terminat cu `\n` (maximum `16 KB`).
2. **Opțional, payload binar brut:** pentru mesajele de transfer de date (`PUT` și răspunsul `FILE` la `GET`), imediat după caracterul `\n` al antetului urmează **exact `dimensiune` octeți** (în flux, în blocuri de `64 KB`, dimensiune maximă fișier **`4 GB`**).

Serverul și clientul citesc antetul octet cu octet până la `\n` (`Protocol.readHeaderLine`) fără `BufferedReader`, garantând că niciun octet din conținutul binar nu este pre-încărcat sau corupt.

---

## 2. Mesaje de transfer fișiere și bucăți (TCP Data Plane)

| Operație | Sens | Antet | Payload după `\n` | Răspuns |
| :--- | :--- | :--- | :--- | :--- |
| **Încărcare** | `Client` $\rightarrow$ `StorageNode` | `PUT <nume_sau_chunkId> <dimensiune> <sha256>` | exact `<dimensiune>` octeți | `STORED <sha256>` sau `ERROR <motiv>` |
| **Descărcare** | `Client` $\rightarrow$ `StorageNode` | `GET <nume_sau_chunkId>` | — | `FILE <dimensiune> <sha256>\n` + `<dimensiune>` octeți, sau `NOT_FOUND`, sau `ERROR <motiv>` |

* Fișierele și bucățile (`<fileUUID>.chunk.<index>`) recepționate sunt scrise întâi într-un fișier temporar `<nume>.part`, cu calcul SHA-256 pe drum, și sunt redenumite atomic (`ATOMIC_MOVE`) doar după ce s-au primit toți octeții și checksum-ul corespunde.

---

## 3. Mesaje de coordonare metadate (HDFS Hybrid Chunking)

| Operație | Sens | Antet cerere | Răspuns |
| :--- | :--- | :--- | :--- |
| **Alocare** | `Client` $\rightarrow$ `Coordonator` | `ALLOCATE <nume> <dimensiune> <sha256>` | `ALLOC_PLAN <uuid> <chunkSize> <chunkCount>` urmat de `<chunkCount>` linii:<br>`CHUNK <i> <chunkId> <offset> <chunkLen> <nodeId>@<host>:<port>`<br>sau `ERROR <motiv>` (`FILE_ALREADY_EXISTS`, `NO_ACTIVE_NODES`, `FILE_TOO_LARGE`, `INVALID_FILENAME`, `INVALID_SIZE`) |
| **Confirmare** | `Client` $\rightarrow$ `Coordonator` | `COMMIT <uuid> <nume> <dimensiune> <fullSha256> <chunkSize> <chunkCount>` urmat de `<chunkCount>` linii:<br>`CHUNK <i> <chunkId> <offset> <chunkLen> <chunkSha256> <noduri_csv>` | `OK` sau `ERROR <motiv>` (`INCOMPLETE_COMMIT`, `INVALID_COMMIT_CHUNK`, `CHUNK_SIZE_MISMATCH`, `FILE_ALREADY_EXISTS`) |
| **Localizare** | `Client` $\rightarrow$ `Coordonator` | `LOCATE <nume_sau_uuid>` | `LOCATIONS <uuid> <nume> <dimensiune> <fullSha256> <chunkSize> <chunkCount>` urmat de `<chunkCount>` linii:<br>`CHUNK <i> <chunkId> <offset> <chunkLen> <chunkSha256> <nod@host:port,...>`<br>sau `NOT_FOUND` / `ERROR CHUNK_OFFLINE <index>` |
| **Listare** | `Client` $\rightarrow$ `Coordonator` | `LIST` | `FILE_LIST <count>` urmat de `<count>` linii:<br>`<uuid> <nume> <dimensiune> <fullSha256> <chunkCount> <chunkSize> <noduri_csv>` |
| **Ștergere** | `Client` $\rightarrow$ `Coordonator` | `DELETE <nume_sau_uuid>` | `DELETED <uuid> <nume> <chunkCount>` sau `NOT_FOUND` |
| **Ștergere bucată** | `Coordonator` / `Client` $\rightarrow$ `StorageNode` | `REMOVE_REPLICA <nume_sau_chunkId>` | `REMOVED <nume_sau_chunkId>` sau `NOT_FOUND <nume_sau_chunkId>` |

---

## 4. Descoperire, Heartbeat și Testare

* **UDP Multicast (`230.0.0.1:6999`) + TCP Unicast fallback (`7001–7005`):** `HEARTBEAT <id> <host> <port> <isCoordinator> <pid>` (la fiecare `5s`).
* **TCP Control:** `NODES_STATUS` $\rightarrow$ `CLUSTER_NODES ...`, `FREEZE` $\rightarrow$ `FROZEN <id>`, `UNFREEZE` $\rightarrow$ `UNFROZEN <id>`, `FORCE_KILL` $\rightarrow$ `KILLED <id>`.

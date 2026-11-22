# Protocol de Comunicație (`sd-storage`)

## 1. Încadrarea mesajelor TCP

Toate mesajele peste **TCP** folosesc același format:
1. **Un antet text UTF-8 pe o singură linie**, terminat cu `\n`.
2. **Opțional, payload binar brut:** pentru mesajele de transfer de date (`PUT` și răspunsul `FILE` la `GET`), imediat după caracterul `\n` al antetului urmează **exact `dimensiune` octeți** (în flux, în blocuri de `64 KB`).

Serverul și clientul citesc antetul octet cu octet până la `\n` (`Protocol.readHeaderLine`) fără `BufferedReader`, garantând că niciun octet din conținutul binar nu este pre-încărcat sau corupt.

---

## 2. Mesaje de transfer fișiere (Etapa 2)

| Operație | Sens | Antet | Payload după `\n` | Răspuns |
| :--- | :--- | :--- | :--- | :--- |
| **Încărcare** | `Client` → `StorageNode` | `PUT <nume> <dimensiune> <sha256>` | exact `<dimensiune>` octeți | `STORED <sha256>` sau `ERROR <motiv>` |
| **Descărcare** | `Client` → `StorageNode` | `GET <nume>` | — | `FILE <dimensiune> <sha256>\n` + `<dimensiune>` octeți, sau `NOT_FOUND`, sau `ERROR <motiv>` |

* Fișierele recepționate sunt scrise întâi într-un fișier temporar `<nume>.part`, cu calcul SHA-256 pe drum, și sunt redenumite atomic (`ATOMIC_MOVE`) doar după ce s-au primit toți octeții și checksum-ul corespunde.

---

## 3. Mesaje de coordonare metadate (Etapa 1 / 2)

| Operație | Sens | Antet | Răspuns |
| :--- | :--- | :--- | :--- |
| **Alocare** | `Client` → `Coordonator` | `ALLOCATE <nume> <dimensiune> <sha256>` | `NODES <uuid> <nodeId>@<host>:<port>` sau `ERROR <motiv>` (`FILE_ALREADY_EXISTS`, `NO_ACTIVE_NODES`, `FILE_TOO_LARGE`) |
| **Confirmare** | `Client` → `Coordonator` | `COMMIT <uuid> <nume> <dimensiune> <sha256> <noduri_csv>` | `OK` sau `ERROR <motiv>` |
| **Localizare** | `Client` → `Coordonator` | `LOCATE <nume_sau_uuid>` | `LOCATIONS <uuid> <nume> <dimensiune> <sha256> <nod@host:port,...>` sau `NOT_FOUND` |
| **Listare** | `Client` → `Coordonator` | `LIST` | `FILE_LIST <count>` urmat de `<count>` linii: `<uuid> <nume> <dimensiune> <sha256> <noduri_csv>` |
| **Ștergere** | `Client` → `Coordonator` | `DELETE <nume_sau_uuid>` | `DELETED <uuid> <nume>` sau `NOT_FOUND` |
| **Ștergere replică** | `Coordonator` → `StorageNode` | `REMOVE_REPLICA <nume>` | `REMOVED <nume>` sau `NOT_FOUND` |

---

## 4. Descoperire, Heartbeat și Testare

* **UDP Multicast (`230.0.0.1:6999`) + TCP Unicast fallback (`7001–7005`):** `HEARTBEAT <id> <host> <port> <isCoordinator> <pid>` (la fiecare `5s`).
* **TCP Control:** `NODES_STATUS` → `CLUSTER_NODES ...`, `FREEZE` → `FROZEN <id>`, `UNFREEZE` → `UNFROZEN <id>`, `FORCE_KILL` → `KILLED <id>`.

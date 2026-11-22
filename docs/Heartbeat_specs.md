# Specificație Tehnică: Descoperirea Nodurilor și Mecanismul de Heartbeat

Acest document descrie modul în care nodurile `StorageNode` se descoperă dinamic în cluster și detectează defectările de tip *Crash-Stop* (*Fail-Stop*) folosind un model hibrid **UDP Multicast + TCP Unicast** bazat pe intervale discrete.

---

## 1. Comparație: Unicast vs. Broadcast vs. Multicast

| Caracteristică | **Unicast (1-la-1)** | **Broadcast (1-la-Toți)** | **Multicast (1-la-un-Grup)** |
| :--- | :--- | :--- | :--- |
| **Destinatari** | Exact **un singur** nod (IP + port). | **Toate** stațiile din LAN/subrețea. | Doar nodurile **abonate** la grupul multicast (`230.0.0.1`). |
| **Protocol tipic** | TCP sau UDP | UDP (`255.255.255.255` / broadcast subrețea) | UDP (Clasa D: `224.0.0.0` – `239.255.255.255`) |
| **Încărcare rețea** | $N$ conexiuni/pachete separate pentru $N$ noduri. | Ridicată (toate plăcile de rețea din LAN procesează pachetul). | Eficientă: emițătorul trimite **un singur pachet**, livrat doar membrilor înscriși (`joinGroup`). |
| **Descoperire dinamică** | Necesită adrese/porturi cunoscute dinainte. | Da, în aceeași subrețea. | Da, nodurile noi se abonează și sunt detectate automat. |

### Abordarea hibridă din `StorageNode`
[StorageNode.java](../src/main/java/ro/ubm/sd/node/StorageNode.java) combină ambele mecanisme în `emitHeartbeatCycle()`:
1. **UDP Multicast (`230.0.0.1:6999`):** permite descoperirea automată a nodurilor noi (ex. `n4`) în rețeaua locală fără configurare statică.
2. **TCP Unicast fallback (porturile `7001–7005` + toate nodurile din `clusterNodes`):** garantează funcționarea pe sisteme unde UDP multicast pe `localhost` este blocat sau filtrat de OS/firewall (ex. Windows).

---

## 2. Formatul mesajului și constantele din cod

### Format mesaj Heartbeat
```text
HEARTBEAT <id> <host> <port> <isCoordinator> <pid>
```
La recepție prin TCP, nodul destinatar actualizează tabela și răspunde cu `OK\n`. Mesajele proprii (`senderId.equals(this.id)`) sunt ignorate.

### Parametri, praguri și timeout-uri

| Constantă / Parametru | Valoare | Rol |
| :--- | :--- | :--- |
| `HEARTBEAT_INTERVAL_SECONDS` | **`5 s`** | Intervalul fix de emisie (`0s, 5s, 10s...`) și de evaluare (`5s, 10s, 15s...`) pe `ScheduledExecutorService` (2 thread-uri daemon). |
| `SUSPECT_INTERVAL_THRESHOLD` | **`3` intervale (`15 s`)** | Pragul după care un nod tăcut trece din `ACTIV` în `SUSPECT`. |
| `CAZUT_INTERVAL_THRESHOLD` | **`5` intervale (`25 s`)** | Pragul după care un nod este declarat `CAZUT` și oprit forțat de coordonator. |
| `MULTICAST_GROUP` / `MULTICAST_PORT` | **`230.0.0.1:6999`** | Grupul și portul UDP pentru descoperire și heartbeat multicast. |
| `DEFAULT_LOCAL_PORTS` | **`7001–7005`** | Porturile locale sondate prin TCP Unicast când `probeDefaultPorts = true`. |
| Timeout conectare TCP (`HEARTBEAT`) | **`400 ms`** | Evită blocarea buclei de emisie când un nod nu ascultă pe port. |
| Timeout conectare TCP (`FORCE_KILL`) | **`300 ms`** | Timeout scurt la trimiterea semnalului `FORCE_KILL`. |
| Timeout așteptare kill OS | **`2 s`** | `proc.waitFor(2, TimeUnit.SECONDS)` după `taskkill` / `kill -9`. |
| Timeout conectare TCP (`Client`) | **`2000 ms`** | Timeout folosit de `Client` la `NODES_STATUS`, `FREEZE`, `UNFREEZE`. |

---

## 3. Mașina de stări (`NodeState`) și ciclul de evaluare

Stările posibile din [NodeState.java](../src/main/java/ro/ubm/sd/common/NodeState.java) sunt `ACTIV`, `SUSPECT` și `CAZUT`, gestionate thread-safe în [NodeInfo.java](../src/main/java/ro/ubm/sd/common/NodeInfo.java):

```text
  [Nod nou descoperit] ──> ACTIV (missedIntervals = 0)
                             │  ▲
   3 intervale ratate (15s)  │  │  Orice HEARTBEAT primit
                             ▼  │  (recordHeartbeat: missedIntervals = 0 -> ACTIV)
                           SUSPECT
                             │  ▲
   5 intervale ratate (25s)  │  │  (daca nu a fost omorat inca si revine)
                             ▼  │
                           CAZUT ──> Coordonatorul trimite FORCE_KILL + kill OS
```

1. **Recepție (`NodeInfo.recordHeartbeat` — `synchronized`):**
   * Actualizează `host`, `port`, `coordinator`, `pid` (dacă `pid > 0`) și `lastHeartbeat = System.currentTimeMillis()`.
   * Setează `receivedInCurrentInterval = true`, `missedIntervals = 0` și readuce starea la **`NodeState.ACTIV`** (inclusiv dacă nodul era `SUSPECT` sau `CAZUT`).
2. **Evaluare (`evaluateNodesIntervalCycle` + `NodeInfo.advanceInterval` — `synchronized`):**
   * Nodul curent își reîmprospătează propria intrare și sare peste nodurile deja marcate `CAZUT`.
   * Dacă `receivedInCurrentInterval == true`: resetează flag-ul pe `false` pentru intervalul următor (`missedIntervals` rămâne `0`).
   * Dacă `receivedInCurrentInterval == false`: incrementează `missedIntervals++`:
     * `1–2` ratări: rămâne `ACTIV` (avertisment în log).
     * `3–4` ratări (`15–20 s`): trece în **`SUSPECT`** (exclus de coordonator la noi alocări `ALLOCATE`).
     * `>= 5` ratări (`25 s`): trece în **`CAZUT`**. Dacă evaluatorul este coordonatorul, apelează `killDeadNodeProcess(node)`.

---

## 4. Oprirea forțată a nodului căzut (`killDeadNodeProcess`)

Când coordonatorul marchează un nod `CAZUT`:
1. Încearcă să trimită pe TCP mesajul **`FORCE_KILL`** (timeout `300 ms`); dacă procesul încă ascultă, acesta răspunde `KILLED <id>` și apelează `System.exit(137)`.
2. Dacă cunoaște `pid`-ul nodului și este diferit de propriul PID, execută la nivel de sistem de operare:
   * **Windows:** `taskkill /F /PID <pid>`
   * **Linux / macOS:** `kill -9 <pid>`

---

## 5. Testarea mecanismului (`freeze` / `unfreeze`)

Pentru simularea blocării unui nod fără a închide fereastra terminalului:

* **Din `Client`:**
  * `nodes` (`NODES_STATUS`) — afișează `ID`, `ADRESA`, `STARE`, `INTERVALE RATATE`, `ULTIMUL HB`, `PID`, `ROL`.
  * `freeze <port|host:port>` — trimite `FREEZE`; nodul setează `heartbeatPaused = true` (răspunde `FROZEN <id>`). După `15s` devine `SUSPECT`, iar la `25s` este marcat `CAZUT` și oprit forțat.
  * `unfreeze <port|host:port>` *(alias `resume`)* — trimite `UNFREEZE`; dacă este apelată înainte de `25s`, nodul reia imediat `emitHeartbeatCycle()` și revine în `ACTIV`.
* **Din consola locală a unui `StorageNode`:**
  * `nodes` / `status`, `freeze` / `pause`, `resume` / `unfreeze`, `exit` / `quit`.

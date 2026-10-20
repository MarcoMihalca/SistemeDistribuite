# Proiect – Etapa 1: Cerințe, Model de Date, Obiective și Protocol

Acest document definește specificațiile funcționale și nefuncționale, modelul metadatelor, scenariul de acceptanță și schița protocolului de comunicație pentru sistemul distribuit de stocare a fișierelor (`sd-storage`).

---

## 1. Cerințe Funcționale: Operațiile Sistemului (Pasul 2)

Sistemul expune operații utilizatorului prin intermediul interfeței în terminal (`Client`) și gestionează evenimente interne ale clusterului (precum adăugarea dinamică a nodurilor). Metadatele sunt gestionate de coordonator, iar transferul efectiv de octeți se realizează direct între client și nodurile de stocare prin TCP.

### Tabelul Operațiilor

| Operație | Comanda | Pașii prin sistem | Rezultat | Erori tratate |
| :--- | :--- | :--- | :--- | :--- |
| **Încărcare** | `put raport.pdf` | 1. Clientul verifică existența și dimensiunea fișierului local și îi calculează checksum-ul SHA-256;<br>2. Clientul cere coordonatorului două noduri active, distincte (`ALLOCATE`);<br>3. Clientul trimite fișierul primului nod (`PUT`);<br>4. Primul nod verifică checksum-ul, salvează local și îl copiază pe al doilea nod (`REPLICATE`);<br>5. După ambele confirmări, primul nod notifică coordonatorul (`COMMIT`), care înregistrează metadatele. | ID-ul fișierului (`UUID`) și cele două noduri pe care a fost stocat (ex. `[n2, n3]`). | • Mai puțin de 2 noduri active: refuz, nimic stocat;<br>• Fișier local inexistent sau dimensiune depășită;<br>• O copie eșuată (nod picat în timpul transferului sau checksum invalid): se alege alt nod activ. |
| **Descărcare** | `get raport.pdf ./out/` | 1. Clientul cere coordonatorului metadatele și locațiile copiilor (`LOCATE`);<br>2. Clientul descarcă fișierul de la primul nod disponibil din listă (`GET`);<br>3. Clientul calculează SHA-256 pe octeții primiți și verifică dacă coincide cu checksum-ul din metadate;<br>4. Salvează fișierul în directorul local `./out/`. | Fișierul salvat local în `./out/`, cu checksum SHA-256 verificat. | • Fișier necunoscut (`NOT_FOUND`);<br>• Primul nod nu răspunde sau checksum greșit: clientul încearcă automat a doua copie de pe celălalt nod;<br>• Nicio copie accesibilă / validă: eroare explicită afișată utilizatorului. |
| **Listarea fișierelor** | `ls` | 1. Clientul trimite cererea `LIST` către coordonator;<br>2. Coordonatorul citește tabela de metadate din memorie;<br>3. Coordonatorul returnează lista completă a fișierelor înregistrate (`FILE_LIST`). | Tabel afișat în terminal cu fiecare fișier: `ID`, `nume`, `dimensiune (octeți)`, `checksum` și `nodurile` care dețin copiile (ex. `[n2, n3]`). | • Coordonator inaccesibil: eroare de conexiune (sau declanșarea redescoperirii liderului în etapele următoare);<br>• Niciun fișier stocat în sistem: se afișează mesajul explicit `(niciun fișier stocat)`. |
| **Ștergerea unui fișier** | `rm raport.pdf` | 1. Clientul trimite comanda `DELETE` (cu numele sau ID-ul fișierului) către coordonator;<br>2. Coordonatorul caută fișierul în tabela de metadate și îl elimină din tabelă (fișierul devine imediat invizibil pentru `ls` și `get`);<br>3. Coordonatorul trimite comanda `REMOVE_REPLICA` către fiecare nod activ care deține o copie;<br>4. Nodurile active șterg fișierul din directorul lor `data/nX/` și confirmă cu `REMOVED`. | Confirmarea ștergerii fișierului și a copiilor sale din sistem (`DELETED`). | • Fișier inexistent în metadate: eroare `NOT_FOUND`;<br>• **Un nod cu o copie este căzut în acel moment**: coordonatorul elimină imediat metadatele fișierului și șterge copia de pe nodul activ. Pentru nodul căzut, coordonatorul fie reține ID-ul fișierului într-o listă de ștergeri în așteptare (*tombstone / pending deletes*) aplicată când nodul revine, fie, la reconectare/sincronizare (Etapa 6), orice fișier raportat de nod care nu mai există în metadatele coordonatorului este considerat *orfan* și șters automat (*garbage collection*). |
| **Starea sistemului** | `nodes` | 1. Clientul trimite cererea `NODES_STATUS` către coordonator;<br>2. Coordonatorul inspectează tabela nodurilor cunoscute, starea lor (`ACTIV`, `SUSPECT`, `CAZUT`), timpul ultimului heartbeat și rolul fiecăruia;<br>3. Coordonatorul răspunde cu `CLUSTER_NODES`. | Afișarea în terminal a tuturor nodurilor cunoscute: `ID`, `adresă:port`, `stare`, timp scurs de la ultimul semn de viață și indicatorul `[COORDONATOR]` pentru liderul curent. | • Coordonator inaccesibil: timeout / eroare de conexiune la interogarea stării. |
| **Adăugarea unui nod nou la rulare** *(perspectiva sistemului)* | *(eveniment intern la pornirea procesului `StorageNode`)* | 1. Noul nod (`n4`) pornește, își creează directorul local `data/n4/` și deschide socket-ul TCP pe portul său;<br>2. Nodul emite un mesaj de descoperire/înregistrare prin UDP multicast (`REGISTER` / `HELLO` cu `id`, `host`, `port`) și începe trimiterea periodică de `HEARTBEAT`;<br>3. Coordonatorul (și celelalte noduri) recepționează anunțul, adaugă `n4` în tabela de noduri cu starea `ACTIV` și timestamp-ul curent;<br>4. De la următoarea comandă `put`, coordonatorul include automat `n4` în algoritmul de selecție și echilibrare a încărcării pentru noile copii. | Nodul `n4` apare în tabela sistemului ca `ACTIV` în cel mult 3 secunde, fără oprirea serviciului, și devine eligibil pentru stocare. | • **Conflict de ID sau port**: dacă există deja un nod `ACTIV` cu același ID dar pe altă adresă/port, coordonatorul respinge înregistrarea (`DUPLICATE_ID`) și noul proces se oprește cu eroare;<br>• **Nod repornit după cădere**: dacă ID-ul exista ca `CAZUT`, este reactivat (`ACTIV`), timestamp-ul este actualizat și își raportează fișierele locale pentru reconciliere. |

### Decizii Arhitecturale Suplimentare

1. **Identificarea unui fișier (Nume vs. ID)**:
   * Intern (la nivelul nodurilor de stocare și în directoarele `data/nX/`), fișierele sunt stocate și identificate unic prin **`UUID`** (de exemplu, `data/n2/3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90`), evitând orice coliziune de nume sau caractere speciale pe disc.
   * În interfața cu utilizatorul (`get`, `rm`), clientul poate specifica fișierul **atât după nume** (ex. `raport.pdf`), **cât și după UUID**. Coordonatorul rezolvă numele în UUID în timpul operațiilor `LOCATE` și `DELETE`.
2. **Încărcarea unui fișier cu nume existent**:
   * Numele de fișiere active sunt **unice** la nivelul sistemului. Dacă utilizatorul execută `put raport.pdf` și există deja un fișier cu numele `raport.pdf` în tabela de metadate, coordonatorul **refuză suprascrierea** cu un mesaj explicit (`ERROR FILE_ALREADY_EXISTS: folosiți rm raport.pdf înainte de reîncărcare`). Această decizie previne pierderea accidentală de date și inconsistențele între copii în caz de eșec parțial.
3. **Dimensiunea maximă a unui fișier**:
   * Sistemul impune o limită maximă configurabilă de **512 MB** (`536_870_912` octeți) per fișier (și minim `1` octet).
   * Transferul pe socket-ul TCP se realizează în flux (*streaming* în blocuri de `64 KB`), astfel încât nici clientul, nici nodurile de stocare să nu încarce întregul fișier în memoria RAM (evitând `OutOfMemoryError`).

---

## 2. Modelul de Date (Pasul 3)

Coordonatorul menține în memorie două structuri principale de date (sincronizate thread-safe cu `ConcurrentHashMap`): **tabela de metadate a fișierelor** și **tabela nodurilor din cluster**.

### 2.1. Metadatele unui fișier (`FileMetadata`)

| Câmp | Tip Java | Exemplu | Stabilit de |
| :--- | :--- | :--- | :--- |
| `id` (identificator) | `UUID` | `3f2a9c1e-5b7d-4e8a-9c21-7d4e5f6a8b90` | Coordonator, la încărcare (`ALLOCATE`) |
| `name` (nume) | `String` | `raport.pdf` | Utilizator (extras din calea fișierului local) |
| `size` (dimensiune) | `long` (octeți) | `482133` | Client (verificat la recepție de noduri) |
| `checksum` | `String` (SHA-256, hex) | `9f86d081884c7d659a2feaa0c55ad015...` | Client (`MessageDigest.getInstance("SHA-256")`), verificat de fiecare nod |
| `replicaNodes` (locațiile copiilor) | `List<String>` (ID-uri de nod) | `["n2", "n3"]` | Coordonator, după confirmările de stocare (`COMMIT`) |

### 2.2. Un nod din listă (`NodeInfo`)

| Câmp | Tip Java | Exemplu | Descriere |
| :--- | :--- | :--- | :--- |
| `id` (ID nod) | `String` | `"n2"` | Identificatorul unic al nodului primit prin `--id` |
| `host` și `port` (adresă și port) | `String`, `int` | `"192.168.37.102"`, `7002` | Adresa IP/hostname și portul TCP pe care ascultă nodul |
| `state` (stare) | `enum NodeState` | `ACTIV`, `SUSPECT`, `CAZUT` | Starea curentă a nodului (gestionată prin heartbeat din Etapa 3) |
| `lastHeartbeat` (ultimul semn de viață) | `long` (ms) | `1760956200000L` | `System.currentTimeMillis()` la ultimul mesaj `HEARTBEAT` primit |
| `coordinator` (rol) | `boolean` | `false` | Indică dacă nodul curent îndeplinește rolul de coordonator |

### 2.3. Planul pentru reconstrucția metadatelor la căderea coordonatorului (Pregătire Etapa 6)

Dacă nodul coordonator cade, tabela de metadate din memoria sa se pierde. Pentru ca noul coordonator (ales prin algoritmul Bully în Etapa 5) să poată reconstrui complet tabela de metadate fără intervenție externă:
* Fiecare nod de stocare salvează pe disc, alături de conținutul fiecărui fișier (`data/nX/<uuid>.bin`), și un mic fișier local de metadate (`data/nX/<uuid>.meta`) care conține: `id`, `name`, `size` și `checksum`.
* Când un nou coordonator preia conducerea (sau când un nod revine în cluster), coordonatorul difuzează o cerere `REPORT_FILES` către toate nodurile active.
* Fiecare nod își scanează directorul `data/nX/` și răspunde cu lista metadatelor locale (`id`, `name`, `size`, `checksum`).
* Noul coordonator agregă rapoartele: pentru fiecare `UUID` raportat, populează `FileMetadata` și adaugă ID-ul nodului raportor în `replicaNodes`. Dacă după agregare un fișier are doar o singură copie pe un nod activ, coordonatorul poate declanșa automat re-replicarea pe un al doilea nod.

---

## 3. Cerințe Nefuncționale și Obiective Măsurabile (Pasul 4)

### Tabelul Obiectivelor Măsurabile

| Cerință | Obiectiv măsurabil (ținta proiectului) | Cum se măsoară | Justificare | Etapa |
| :--- | :--- | :--- | :--- | :--- |
| **Integritate** | **0 fișiere corupte** (cu checksum SHA-256 diferit) din 100 de încărcări și descărcări succesive de dimensiuni între 1 KB și 50 MB. | Script automat care încarcă 100 de fișiere, le descarcă în `./out/` și compară SHA-256 inițial cu cel descărcat. | TCP garantează ordinea octeților, iar validarea SHA-256 la fiecare pas (`PUT`, `REPLICATE`, `GET`) elimină orice trunchiere sau corupere pe disc/rețea. | 2 |
| **Descoperire** | Un nod nou pornit apare în lista `nodes` a celorlalte noduri și a coordonatorului în **cel mult 3 secunde**. | Diferența dintre timestamp-ul de pornire din logul nodului nou și timestamp-ul de înregistrare din logul coordonatorului. | Anunțul UDP multicast se trimite imediat la pornire și se repetă la un interval de 1 secundă, asigurând detecția sub 3 s pe LAN. | 2 |
| **Detectarea defectelor** | Un nod oprit forțat (`kill -9`) trece în starea `SUSPECT` în max. **3 s** și este marcat `CAZUT` în **cel mult 5 secunde**. | Diferența dintre momentul comenzii `kill -9` și timestamp-ul tranziției în `CAZUT` din logul coordonatorului. | Cu un interval de `HEARTBEAT` de 1 s, lipsa a 3 mesaje consecutive (3 s) marchează nodul `SUSPECT`, iar la 5 s fără răspuns este declarat `CAZUT`. | 3 |
| **Scalabilitate** | Debitul agregat de încărcare/descărcare cu **3 noduri crește cu cel puțin 35%** față de configurația cu 2 noduri pentru încărcări distribuite. | Măsurarea timpului total (wall-clock) pentru încărcarea a 10 fișiere de câte 100 MB (1 GB total) cu 2 noduri vs. 3/4 noduri. | Deoarece datele circulă direct între client și nodurile de stocare (fără a trece prin coordonator), distribuirea copiilor pe mai multe noduri reduce contesiunea pe disc și I/O. | 4 |
| **Coordonare** | Noul coordonator este ales prin algoritmul Bully și devine operațional în **cel mult 8 secunde** după căderea vechiului coordonator. | Diferența în loguri între oprirea vechiului coordonator (detectată în ~5 s) și mesajul `COORDINATOR` emis de noul lider (timeout de elecție ~2–3 s). | Suma dintre timpul de detecție a căderii liderului (max. 5 s) și runda de mesaje `ELECTION` / `OK` / `COORDINATOR` pe rețea locală (< 3 s). | 5 |
| **Replicare** | **100% din fișierele confirmate** au exact **2 copii** stocate pe noduri fizice/logice distincte (când există $\ge 2$ noduri active). | Verificarea ieșirii comenzii `ls` și inspectarea directoarelor `data/n1/`, `data/n2/`, `data/n3/`. | Coordonatorul nu marchează un fișier ca fiind stocat decât după confirmarea ambelor copii (primară + replică) pe ID-uri de nod diferite. | 6 |
| **Disponibilitate** | După oprirea neplanificată a oricărui singur nod (inclusiv coordonatorul), **100% din fișierele încărcate rămân descărcabile**. | Oprirea unui nod cu `kill -9` urmată de rularea comenzii `get` pentru toate fișierele din sistem și validarea SHA-256. | Factorul de replicare $R = 2$ pe noduri distincte plus reconstrucția metadatelor la realegerea liderului garantează toleranța la 1 defect (*single-node failure*). | 7 |

### Ipoteze de Lucru (Assumptions)

Obiectivele măsurabile de mai sus sunt definite și garantate în interiorul următoarelor ipoteze de proiectare:
1. **Rețea locală (LAN / localhost)**: Nodurile comunică într-o rețea locală cu latență scăzută ($< 10\text{ ms}$) și care permite traficul UDP multicast (pentru descoperire). Nu există partiționări prelungite ale rețelei (*split-brain*).
2. **Modelul de defecte este *Crash-Stop* (*Fail-Stop*), nu Bizantin**: Nodurile se pot opri brusc (`kill -9`, cădere proces) și pot reporni ulterior, dar nu trimit mesaje falsificate intenționat și nu alterează malițios protocolul.
3. **Cel mult un nod căzut simultan ($f = 1$) între ferestrele de recuperare**: Având factorul de replicare $R = 2$, sistemul garantează disponibilitatea de 100% a datelor atâta timp cât nu cad simultan ambele noduri care dețin cele două copii ale aceluiași fișier.
4. **Minimum 2 noduri active pentru scriere**: Operația `put` necesită cel puțin 2 noduri în starea `ACTIV` în cluster; în caz contrar, scrierea este refuzată în mod sigur (*fail-safe*).
5. **Ceasuri rezonabil sincronizate / măsurători bazate pe intervale relative**: Detecția defectelor folosește ceasul monoton local al fiecărui nod (durata de la ultimul heartbeat recepționat), fără a depinde de sincronizarea perfectă a ceasurilor fizice (NTP) între mașini diferite, cu excepția corelării logurilor pentru benchmark.

---

## 4. Scenariul Demonstrației (Pasul 5)

Acest scenariu reprezintă testul de acceptanță cap-coadă al proiectului, rulat incremental la fiecare etapă și demonstrat complet în săptămâna 14.

*(Notă: Pe Linux/macOS/Git Bash se folosește `kill -9 <PID>`, iar pe Windows PowerShell echivalentul `Stop-Process -Id <PID> -Force`).*

| Pas | Acțiunea operatorului | Comanda exactă rulată în terminal | Ce trebuie să se vadă | Funcționează din etapa |
| :---: | :--- | :--- | :--- | :---: |
| **1** | Pornește nodurile `n1`, `n2`, `n3` în 3 terminale separate și clientul în al 4-lea terminal. | **T1:** `java -cp target/classes ro.ubm.sd.node.StorageNode --id n1 --port 7001 --dir data/n1 --coordinator`<br>**T2:** `java -cp target/classes ro.ubm.sd.node.StorageNode --id n2 --port 7002 --dir data/n2`<br>**T3:** `java -cp target/classes ro.ubm.sd.node.StorageNode --id n3 --port 7003 --dir data/n3`<br>**T4:** `java -cp target/classes ro.ubm.sd.client.Client --coordinator localhost:7001`<br>În client: `nodes` | Fiecare nod afișează configurația la pornire și îi descoperă automat pe ceilalți; `n1` este marcat coordonator, iar `n1, n2, n3` apar ca `ACTIV`. | **2** |
| **2** | Încarcă fișierul `raport.pdf` din terminalul clientului. | În client (T4):<br>`put raport.pdf` | Clientul afișează confirmarea încărcării, `UUID`-ul generat pentru `raport.pdf`, checksum-ul SHA-256 și cele **două noduri distincte** care îl dețin (ex. `[n2, n3]`). | **2** |
| **3** | Descarcă fișierul `raport.pdf` în directorul `./out/`. | În client (T4):<br>`get raport.pdf ./out/` | Fișierul este descărcat în `./out/raport.pdf`; clientul confirmă că checksum-ul SHA-256 calculat local este **identic** cu cel din metadate. | **2** |
| **4** | Pornește un al patrulea nod (`n4`) într-un terminal nou, fără a opri niciun proces existent. | **T5:** `java -cp target/classes ro.ubm.sd.node.StorageNode --id n4 --port 7004 --dir data/n4`<br>În client (T4):<br>`nodes`<br>`put arhiva.zip` | În max. 3 s, `n4` apare automat în `nodes` cu starea `ACTIV`. La încărcarea `put arhiva.zip`, coordonatorul îl folosește pe `n4` pentru distribuirea încărcării. | **4** |
| **5** | Oprește forțat (`kill -9`) unul dintre nodurile de stocare care deține o copie a lui `raport.pdf` (ex. `n2`) și descarcă fișierul. | În terminalul OS:<br>`kill -9 <PID_n2>` *(sau `Stop-Process -Id <PID_n2> -Force`)*<br>După 5 s, în client (T4):<br>`nodes`<br>`get raport.pdf ./out/` | În `nodes`, nodul `n2` este marcat `CAZUT`. Comanda `get raport.pdf ./out/` reușește transparent prin preluarea celeilalte copii (ex. de pe `n3`) și verifică checksum-ul. | **3** |
| **6** | Oprește forțat (`kill -9`) nodul coordonator (`n1`). | În terminalul OS:<br>`kill -9 <PID_n1>` *(sau `Stop-Process -Id <PID_n1> -Force`)* | În logurile nodurilor rămase (`n3`, `n4`) se observă detectarea căderii lui `n1`, declanșarea algoritmului **Bully** și alegerea noului coordonator (nodul activ cu ID maxim, ex. `n4`). | **5** |
| **7** | Execută `ls` și `get raport.pdf` după realegerea coordonatorului. | În client (T4):<br>`nodes`<br>`ls`<br>`get raport.pdf ./out/` | Noul coordonator a reconstruit tabela de metadate din raportările nodurilor rămase; `ls` afișează `raport.pdf`, iar `get raport.pdf ./out/` descarcă fișierul validat cu succes. | **6** |

---

## 5. Schița Protocolului de Comunicație (Pasul 6)

Comunicația pentru operațiile pe fișiere și metadate se realizează peste **TCP**. Fiecare mesaj are un **antet pe o singură linie** (text UTF-8 terminat cu `\n` sau JSON pe o linie), iar în cazul mesajelor care transportă date (`PUT`, `REPLICATE`, răspunsul la `GET`), imediat după `\n`-ul antetului urmează **exact `size` octeți** reprezentând conținutul brut al fișierului.

### Tabelul Mesajelor Protocolului (Etapa 1 / Etapa 2)

| Mesaj | De la $\rightarrow$ Către | Conținut (Antet + Payload) | Răspuns |
| :--- | :--- | :--- | :--- |
| **`ALLOCATE`** | `client` $\rightarrow$ `coordonator` | `ALLOCATE <nume> <dimensiune> <checksum>` | • `NODES <id_fișier> <nod1_id@host:port> <nod2_id@host:port>`<br>• sau `ERROR <motiv>` (ex. `INSUFFICIENT_NODES`, `FILE_ALREADY_EXISTS`, `FILE_TOO_LARGE`) |
| **`PUT`** | `client` $\rightarrow$ `nod1` | `PUT <id_fișier> <nume> <dimensiune> <checksum> <nod2_id@host:port>\n` + `<octeții fișierului>` | • `STORED <id_fișier>` (după salvarea locală, replicarea pe `nod2` și `COMMIT`)<br>• sau `ERROR <motiv>` (ex. `CHECKSUM_MISMATCH`, `REPLICATION_FAILED`, `IO_ERROR`) |
| **`REPLICATE`** | `nod1` $\rightarrow$ `nod2` | `REPLICATE <id_fișier> <nume> <dimensiune> <checksum>\n` + `<octeții fișierului>` *(ca `PUT`, fără nodul următor)* | • `STORED <id_fișier>`<br>• sau `ERROR <motiv>` (ex. `CHECKSUM_MISMATCH`, `IO_ERROR`) |
| **`COMMIT`** | `nod1` $\rightarrow$ `coordonator` | `COMMIT <id_fișier> <nume> <dimensiune> <checksum> <nod1_id>,<nod2_id>` | • `OK`<br>• sau `ERROR <motiv>` |
| **`LOCATE`** | `client` $\rightarrow$ `coordonator` | `LOCATE <nume_sau_id>` | • `LOCATIONS <id_fișier> <nume> <dimensiune> <checksum> <nod1_id@host:port>,<nod2_id@host:port>`<br>• sau `NOT_FOUND` |
| **`GET`** | `client` $\rightarrow$ `nod` | `GET <id_fișier>` | • `FILE_DATA <id_fișier> <dimensiune> <checksum>\n` + `<octeții fișierului>`<br>• sau `NOT_FOUND` / `ERROR <motiv>` |
| **`LIST`** *(pentru `ls`)* | `client` $\rightarrow$ `coordonator` | `LIST` | • `FILE_LIST <număr_fișiere>\n` urmat de câte o linie per fișier: `<id_fișier> <nume> <dimensiune> <checksum> <nod1_id>,<nod2_id>`<br>• (dacă nu există fișiere: `FILE_LIST 0`) |
| **`DELETE`** *(pentru `rm`)* | `client` $\rightarrow$ `coordonator` | `DELETE <nume_sau_id>` | • `DELETED <id_fișier> <nume>`<br>• sau `NOT_FOUND` / `ERROR <motiv>` |
| **`REMOVE_REPLICA`** *(intern la `rm`)* | `coordonator` $\rightarrow$ `nod` | `REMOVE_REPLICA <id_fișier>` | • `REMOVED <id_fișier>`<br>• sau `NOT_FOUND` / `ERROR <motiv>` |
| **`NODES_STATUS`** *(pentru `nodes`)* | `client` $\rightarrow$ `coordonator` | `NODES_STATUS` | • `CLUSTER_NODES <număr_noduri> <id_coordonator>\n` urmat de câte o linie per nod: `<id_nod> <host>:<port> <stare> <ultimul_heartbeat_ms>` |

*(Notă: Mesajele de descoperire prin UDP multicast `DISCOVER` / `REGISTER`, mesajele periodice `HEARTBEAT`, mesajele de alegere a liderului Bully `ELECTION` / `ALIVE` / `COORDINATOR` și mesajele de reconstrucție `REPORT_FILES` vor extinde acest protocol în Etapele 2, 3, 5 și 6).*

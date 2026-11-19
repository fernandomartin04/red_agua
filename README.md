# Red de Distribución de Agua — Sistemas Distribuidos

Simulación y monitorización en tiempo real de una red de distribución de agua con **5 tramos (`T1..T5`)** y **15 sensores** (3 por tramo: `caudal_entrada`, `caudal_salida` y `presion`), transporte tolerante a fallos mediante **Apache Kafka 4.3.2 (KRaft)**, almacenamiento en **TimescaleDB (PostgreSQL 16)** y visualización en vivo en **Node-RED Dashboard 2.0** (100 % libre de nodos `function`).

---

## 1. Topología Física y Sensores (15 sensores)

```mermaid
flowchart LR
    DEP["Depósito Central"] --> T1["Tramo T1<br/>(Q_in, Q_out, P)"]
    T1 --> U1(("Unión U1"))
    U1 --> T2["Tramo T2<br/>(Q_in, Q_out, P)"]
    U1 --> T3["Tramo T3<br/>(Q_in, Q_out, P)"]
    T2 --> U2(("Unión U2"))
    U2 --> T4["Tramo T4<br/>(Q_in, Q_out, P)"]
    T4 --> BA["Barrio A"]
    T3 --> U3(("Unión U3"))
    U3 --> T5["Tramo T5<br/>(Q_in, Q_out, P)"]
    T5 --> BB["Barrio B"]
```

* **Conservación de caudales en las uniones:**
  * Demanda sinusoidal del Barrio A: $a(t) = 30 + 7{,}5 \sin(\omega t)$
  * Demanda sinusoidal del Barrio B: $b(t) = 30 + 7{,}5 \sin(\omega t + 1{,}6)$
  * Caudales base por tramo: $Q_{T1} = a(t) + b(t)$, $Q_{T2} = Q_{T4} = a(t)$, $Q_{T3} = Q_{T5} = b(t)$.

---

## 2. Diagrama de Arquitectura del Sistema

```mermaid
flowchart TD
    subgraph Nodos["Contenedores de Sensores (5 Nodos)"]
        N1["tramo1 (T1)"]
        N2["tramo2 (T2)"]
        N3["tramo3 (T3)"]
        N4["tramo4 (T4)"]
        N5["tramo5 (T5)"]
    end

    subgraph Kafka["Clúster Apache Kafka 4.3.2 (KRaft · RF=3 · min.isr=2)"]
        K1["kafka1 (:9092)"]
        K2["kafka2 (:9094)"]
        K3["kafka3 (:9096)"]
        TOP["Topic: lecturas (6 particiones)<br/>Topic: lecturas-dlq (1 partición)<br/>Topic: alarmas (3 particiones)"]
    end

    subgraph Persistencia["Persistencia y Almacenamiento"]
        PER["persistor.py<br/>(Lotes 500 msgs / 200 ms · Dedup en RAM)"]
        DB[("TimescaleDB (PG 16 :5432)<br/>Hypertable: lecturas<br/>PK: (ts_tick, tramo, sensor)")]
    end

    subgraph Visualizacion["Monitorización en Vivo (<= 5 s)"]
        NR["Node-RED v5 (:1880) + Dashboard 2.0<br/>(Sin nodos function: switch + change + ui-chart + ui-gauge)"]
    end

    N1 & N2 & N3 & N4 & N5 -->|"acks=all, idempotente<br/>murmur2_random"| TOP
    TOP -->|"enable.auto.commit=False"| PER
    PER -->|"1º conn.commit() (UPSERT)<br/>2º consumidor.commit()"| DB
    TOP -->|"fromOffset: latest<br/>UUID group"| NR
```

---

## 3. Guía de Instalación y Puesta en Marcha

### Requisitos previos
* Docker y Docker Compose v2
* Python 3.10+ y Node.js 18+ (con Node-RED instalado)

### Paso 1: Levantar la infraestructura base (Kafka KRaft + TimescaleDB)
```bash
cd ~/Escritorio/sistemas_distribuidos/red_agua
cp -n .env.example .env   # Ajusta tus credenciales en .env si es la primera vez
docker compose up -d
```
Verifica que los 3 brokers (`kafka1`, `kafka2`, `kafka3`), `kafka-ui` (`:8080`) y `timescaledb` (`:5432`) estén `healthy`:
```bash
docker compose ps
```

### Paso 2: Iniciar los 5 nodos de tramos y el persistidor
Puedes lanzarlos de **dos formas equivalentes**:

#### Opción A — Como 5 contenedores independientes en Docker (perfil `nodos`):
```bash
docker compose --profile nodos up -d --build
```
*(Esto levanta los contenedores `tramo1`, `tramo2`, `tramo3`, `tramo4`, `tramo5` y `persistor`).*

#### Opción B — En terminales locales (ideal para depurar y hacer pruebas en directo):
```bash
# Terminal 1: Persistidor Kafka -> TimescaleDB
pruebas/.venv/bin/python persistencia/persistor.py --grupo persistor-vivo

# Terminal 2: Simulador de los 5 tramos (1 tick/s por tramo)
pruebas/.venv/bin/python motor/simulador.py
```

### Paso 3: Levantar el Dashboard en vivo en Node-RED
```bash
(cd nodered && npm install)
node-red -p 1880 -u nodered nodered/flows.json
```
* **Dashboard en vivo:** [http://localhost:1880/dashboard/vivo](http://localhost:1880/dashboard/vivo)
* **Editor de flujos:** [http://localhost:1880](http://localhost:1880)
* **Kafka UI:** [http://localhost:8080](http://localhost:8080)

---

## 4. Ejecución de Tests y Pruebas de Fallos

### Tests automáticos (`pytest`)
```bash
pruebas/.venv/bin/pytest -v tests/
```

### Pruebas de tolerancia a fallos (A–G)
Ver el detalle paso a paso en [pruebas_fallos/GUIA.md](pruebas_fallos/GUIA.md) y las evidencias en `imagenes-doc/`.

### Comprobar latencia extremo a extremo en TimescaleDB ($\le 5\text{ s}$)
```bash
docker exec timescaledb psql -U postgres -d red_agua -c "
SELECT tramo, sensor, valor, ts_real, ts_bd,
       ROUND(EXTRACT(EPOCH FROM (ts_bd - ts_real)) * 1000, 2) AS latencia_ms
FROM lecturas
ORDER BY ts_tick DESC LIMIT 10;"
```

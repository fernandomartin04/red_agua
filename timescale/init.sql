-- Extensión TimescaleDB
CREATE EXTENSION IF NOT EXISTS timescaledb CASCADE;

-- Tabla principal de lecturas de sensores (15 sensores = 5 tramos x 3 sensores)
-- Justificación de diseño para la defensa:
-- 1. En TimescaleDB, la clave primaria de una hypertable DEBE incluir la columna de tiempo (ts_tick).
-- 2. La clave primaria es (ts_tick, tramo, sensor) para deduplicación idempotente.
-- 3. ts_tick se calcula en microsegundos enteros: T0 + (tick * 1000000) // tps.
-- 4. ts_real es la marca de tiempo del reloj del contenedor productor al emitir;
--    ts_bd es la marca de tiempo al insertar en la BD (permite medir latencia ts_bd - ts_real).
-- 5. valor admite NULL cuando estado = 'SIN_DATO' (sensor averiado sin descartar el resto del tramo).
CREATE TABLE IF NOT EXISTS lecturas (
    ts_tick TIMESTAMPTZ NOT NULL,
    ts_real TIMESTAMPTZ NOT NULL,
    ts_bd   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    tramo   VARCHAR(10) NOT NULL,
    sensor  VARCHAR(30) NOT NULL,
    valor   DOUBLE PRECISION NULL,
    estado  VARCHAR(20) NOT NULL DEFAULT 'OK',
    tick    BIGINT NOT NULL,
    boot_id VARCHAR(32),
    seq     BIGINT,
    epoch   BIGINT NOT NULL DEFAULT 0,
    lamport BIGINT NOT NULL DEFAULT 0,
    CONSTRAINT pk_lecturas PRIMARY KEY (ts_tick, tramo, sensor)
);

-- Convertir en hypertable particionada por ts_tick (intervalos de 1 día)
SELECT create_hypertable('lecturas', 'ts_tick', chunk_time_interval => INTERVAL '1 day', if_not_exists => TRUE);

-- Índice secundario para consultas por tramo y sensor ordenadas por tiempo reciente
CREATE INDEX IF NOT EXISTS idx_lecturas_tramo_sensor_ts 
ON lecturas (tramo, sensor, ts_tick DESC);

-- Tabla de alarmas generadas
-- elemento identifica la fuente ('T1-presion', 'T3', 'U1', 'GLOBAL') y es NOT NULL
-- para que el índice único parcial evite abrir alarmas repetidas en cada tick.
CREATE TABLE IF NOT EXISTS alarmas (
    id          BIGSERIAL PRIMARY KEY,
    ts_alerta   TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    ts_fin      TIMESTAMPTZ NULL,
    ts_tick     TIMESTAMPTZ,
    elemento    VARCHAR(30) NOT NULL,
    tramo       VARCHAR(10),
    sensor      VARCHAR(30),
    tipo        VARCHAR(50) NOT NULL,
    valor       DOUBLE PRECISION,
    umbral      DOUBLE PRECISION,
    severidad   VARCHAR(20) DEFAULT 'WARNING',
    mensaje     TEXT,
    ack         BOOLEAN DEFAULT FALSE,
    ack_ts      TIMESTAMPTZ,
    ack_usuario VARCHAR(50)
);

CREATE UNIQUE INDEX IF NOT EXISTS idx_alarmas_abiertas_unica
ON alarmas (elemento, tipo) WHERE ts_fin IS NULL;

CREATE INDEX IF NOT EXISTS idx_alarmas_tramo_ts ON alarmas (tramo, ts_alerta DESC);
CREATE INDEX IF NOT EXISTS idx_alarmas_pendientes ON alarmas (ts_alerta DESC) WHERE ack = FALSE;

-- Vista para balance de caudales por tramo (detección de fugas):
-- Si un sensor está en SIN_DATO (valor NULL), balance_fuga da NULL (INDETERMINADO).
CREATE OR REPLACE VIEW vista_balance_caudal AS
SELECT 
    e.ts_tick,
    e.tramo,
    e.valor AS caudal_entrada,
    s.valor AS caudal_salida,
    (e.valor - s.valor) AS balance_fuga
FROM lecturas e
JOIN lecturas s ON e.ts_tick = s.ts_tick 
               AND e.tramo = s.tramo 
               AND e.sensor = 'caudal_entrada' 
               AND s.sensor = 'caudal_salida';

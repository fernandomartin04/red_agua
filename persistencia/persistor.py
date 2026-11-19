"""Persistidor Kafka -> TimescaleDB por lotes con deduplicación, DLQ e idempotencia.

Decisiones técnicas para la defensa:
1. Consume por lotes (hasta 500 mensajes o 200 ms) con consume() de librdkafka.
2. Deduplica en memoria en un diccionario por clave primaria (ts_tick, tramo, sensor)
   ANTES del INSERT para evitar el error de PostgreSQL:
   'ON CONFLICT DO UPDATE command cannot affect row a second time'.
3. Inserta con execute_values + ON CONFLICT (ts_tick, tramo, sensor) DO UPDATE
   (dejando ts_bd intacto en reentregas y protegiendo con WHERE EXCLUDED.epoch >= lecturas.epoch).
4. Si un sensor viene como 'sin_dato' o falta en el mensaje del tramo, se guarda
   con valor = NULL y estado = 'SIN_DATO' sin perder los demás sensores del tramo.
5. Si un mensaje está corrupto o viola longitudes, se envía a 'lecturas-dlq'
   para que un mensaje venenoso nunca bloquee el consumidor en bucle.
6. Hace conn.commit() en TimescaleDB ANTES de consumidor.commit() en Kafka.
"""
import argparse
import json
import os
import signal
import time
from datetime import datetime, timezone

import psycopg2
from psycopg2.extras import execute_values
from confluent_kafka import Consumer, Producer, KafkaException, KafkaError

ERRORES_COMMIT_TEMPORALES = {
    KafkaError.NOT_COORDINATOR,
    KafkaError.COORDINATOR_NOT_AVAILABLE,
    KafkaError.COORDINATOR_LOAD_IN_PROGRESS,
    KafkaError.REBALANCE_IN_PROGRESS,
    KafkaError.ILLEGAL_GENERATION,
    KafkaError.UNKNOWN_MEMBER_ID,
    KafkaError._TIMED_OUT,
}

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")
TOPIC = "lecturas"
TOPIC_DLQ = "lecturas-dlq"

PG_HOST = os.getenv("POSTGRES_HOST", "localhost")
PG_PORT = int(os.getenv("POSTGRES_PORT", os.getenv("TIMESCALE_PORT", "5432")))
PG_DB = os.getenv("POSTGRES_DB", "red_agua")
PG_USER = os.getenv("POSTGRES_USER", "postgres")
PG_PASS = os.getenv("POSTGRES_PASSWORD", "postgres")

T0 = int(float(os.getenv("T0", "1767225600")))
TPS = float(os.getenv("TPS", "1.0"))

SENSORES_CONOCIDOS = ("caudal_entrada", "caudal_salida", "presion")

SQL_UPSERT = """
INSERT INTO lecturas (
    ts_tick, ts_real, tramo, sensor, valor, estado, tick, boot_id, seq, epoch, lamport
)
VALUES %s
ON CONFLICT (ts_tick, tramo, sensor) DO UPDATE SET
    ts_real = EXCLUDED.ts_real,
    valor   = EXCLUDED.valor,
    estado  = EXCLUDED.estado,
    tick    = EXCLUDED.tick,
    boot_id = EXCLUDED.boot_id,
    seq     = EXCLUDED.seq,
    epoch   = EXCLUDED.epoch,
    lamport = EXCLUDED.lamport
WHERE EXCLUDED.epoch >= lecturas.epoch;
"""


def conectar_bd():
    while True:
        try:
            conn = psycopg2.connect(
                host=PG_HOST,
                port=PG_PORT,
                dbname=PG_DB,
                user=PG_USER,
                password=PG_PASS,
            )
            conn.autocommit = False
            print(f"[BD] Conectado a TimescaleDB en {PG_HOST}:{PG_PORT}/{PG_DB}")
            return conn
        except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
            print(f"[BD] Error conectando ({e}), reintentando en 2 s...")
            time.sleep(2.0)


def calcular_ts_tick(datos, tick, tps):
    """Devuelve ts_tick como datetime UTC con precisión de microsegundos enteros."""
    if "ts_tick_us" in datos:
        us = int(datos["ts_tick_us"])
    elif "ts_tick" in datos:
        us = round(float(datos["ts_tick"]) * 1_000_000)
    else:
        us = T0 * 1_000_000 + round((tick * 1_000_000) / tps)
    return datetime.fromtimestamp(us / 1_000_000, tz=timezone.utc)


def parsear_valor_sensor(raw):
    """Convierte el valor de un sensor a (float | None, estado)."""
    if raw is None or raw == "sin_dato":
        return None, "SIN_DATO"
    return float(raw), "OK"


def extraer_filas(datos, tps=TPS, modo_prueba=False):
    """Extrae tuplas (ts_tick, ts_real, tramo, sensor, valor, estado, tick, boot_id, seq, epoch, lamport)."""
    tramo = str(datos["tramo"])
    if not tramo or len(tramo) > 10:
        raise ValueError(f"tramo inválido: {tramo!r}")

    tick = int(datos["tick"])
    boot_id = str(datos["boot_id"]) if datos.get("boot_id") is not None else None
    if boot_id is not None and len(boot_id) > 32:
        raise ValueError(f"boot_id excede 32 caracteres: {boot_id!r}")

    seq = int(datos["seq"]) if datos.get("seq") is not None else None
    epoch = int(datos.get("epoch", 0))
    lamport = int(datos.get("lamport", 0))

    ts_tick = calcular_ts_tick(datos, tick, tps)
    ts_real_epoch = float(datos.get("ts_real", time.time()))
    ts_real = datetime.fromtimestamp(ts_real_epoch, tz=timezone.utc)

    pares_sensor = []
    if isinstance(datos.get("lecturas"), dict):
        lect = datos["lecturas"]
        for s in SENSORES_CONOCIDOS:
            val, est = parsear_valor_sensor(lect.get(s, "sin_dato"))
            pares_sensor.append((s, val, est))
    elif "sensor" in datos and ("valor" in datos or "estado" in datos):
        s = str(datos["sensor"])
        if len(s) > 30:
            raise ValueError(f"sensor excede 30 caracteres: {s!r}")
        val, est = parsear_valor_sensor(datos.get("valor"))
        pares_sensor.append((s, val, est))
    elif any(s in datos for s in SENSORES_CONOCIDOS):
        for s in SENSORES_CONOCIDOS:
            val, est = parsear_valor_sensor(datos.get(s, "sin_dato"))
            pares_sensor.append((s, val, est))
    elif modo_prueba and "seq" in datos:
        pares_sensor.append(("seq_prueba", float(seq), "OK"))
    else:
        raise ValueError("Mensaje sin sensores conocidos y modo_prueba desactivado")

    filas = []
    for sensor, valor, estado in pares_sensor:
        filas.append((
            ts_tick, ts_real, tramo, sensor, valor, estado,
            tick, boot_id, seq, epoch, lamport
        ))
    return filas


def al_asignar(consumidor, particiones):
    print("[REBALANCEO] asignadas:", sorted(p.partition for p in particiones))


def al_revocar(consumidor, particiones):
    print("[REBALANCEO] revocadas:", sorted(p.partition for p in particiones))


def main():
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    parser = argparse.ArgumentParser()
    parser.add_argument("--grupo", default="persistor-timescaledb")
    parser.add_argument("--lote-max", type=int, default=500)
    parser.add_argument("--lote-timeout", type=float, default=0.2)
    parser.add_argument("--tps", type=float, default=TPS)
    parser.add_argument("--modo-prueba", action="store_true",
                        help="Acepta mensajes sin sensores de productor_prueba.py como seq_prueba")
    parser.add_argument("--max-mensajes", type=int, default=0,
                        help="Si > 0, termina tras procesar N mensajes de Kafka")
    args = parser.parse_args()

    conn = conectar_bd()
    consumidor = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.grupo,
        "client.id": f"persistor-{args.grupo}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    productor_dlq = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": f"persistor-dlq-{args.grupo}",
        "acks": "all",
    })

    consumidor.subscribe([TOPIC], on_assign=al_asignar, on_revoke=al_revocar)
    print(f"Persistidor activo · grupo={args.grupo} · lote={args.lote_max} msgs / "
          f"{args.lote_timeout*1000:.0f} ms · modo_prueba={args.modo_prueba}")

    msgs_kafka = 0
    filas_insertadas = 0
    colisiones_lote = 0
    enviados_dlq = 0
    lotes_confirmados = 0
    ultimo_informe = time.monotonic()

    try:
        while True:
            mensajes = consumidor.consume(num_messages=args.lote_max, timeout=args.lote_timeout)
            if not mensajes:
                continue

            lote_dedup = {}
            msgs_validos = 0

            for msg in mensajes:
                if msg.error():
                    print("[KAFKA ERROR]", msg.error())
                    continue
                try:
                    datos = json.loads(msg.value())
                    for fila in extraer_filas(datos, args.tps, modo_prueba=args.modo_prueba):
                        clave_pk = (fila[0], fila[2], fila[3])  # (ts_tick, tramo, sensor)
                        if clave_pk in lote_dedup:
                            colisiones_lote += 1
                        lote_dedup[clave_pk] = fila
                    msgs_validos += 1
                except Exception as e:
                    enviados_dlq += 1
                    print(f"[DLQ] offset={msg.offset()} part={msg.partition()} error={e}")
                    try:
                        envoltorio_dlq = {
                            "error": str(e),
                            "partition": msg.partition(),
                            "offset": msg.offset(),
                            "raw": msg.value().decode("utf-8", errors="replace") if msg.value() else None,
                        }
                        productor_dlq.produce(TOPIC_DLQ, key=msg.key(), value=json.dumps(envoltorio_dlq))
                        productor_dlq.poll(0)
                    except Exception as dlq_err:
                        print(f"[DLQ ERROR] {dlq_err}")

            if lote_dedup:
                guardado = False
                while not guardado:
                    try:
                        with conn.cursor() as cur:
                            execute_values(cur, SQL_UPSERT, list(lote_dedup.values()), page_size=500)
                        conn.commit()
                        guardado = True
                    except (psycopg2.OperationalError, psycopg2.InterfaceError) as e:
                        print(f"[BD] Caída de conexión durante INSERT ({e}), reconectando...")
                        try:
                            conn.close()
                        except Exception:
                            pass
                        conn = conectar_bd()
                    except Exception as e:
                        try:
                            conn.rollback()
                        except Exception:
                            pass
                        raise e

            # Confirmar offsets en Kafka después del commit en BD (o tras desviar a DLQ)
            try:
                consumidor.commit(asynchronous=False)
            except KafkaException as e:
                err = e.args[0]
                if err.code() in ERRORES_COMMIT_TEMPORALES or err.retriable():
                    print(f"[COMMIT] fallo temporal ({err.name()}), BD ya es idempotente")
                else:
                    raise

            msgs_kafka += msgs_validos
            filas_insertadas += len(lote_dedup)
            lotes_confirmados += 1

            ahora = time.monotonic()
            if ahora - ultimo_informe >= 5.0:
                print(f"[PERSISTOR] msgs_kafka={msgs_kafka} filas_bd={filas_insertadas} "
                      f"lotes={lotes_confirmados} dedup_en_lote={colisiones_lote} dlq={enviados_dlq}")
                ultimo_informe = ahora

            if args.max_mensajes > 0 and msgs_kafka >= args.max_mensajes:
                print(f"Alcanzado límite de prueba ({msgs_kafka} mensajes). Saliendo...")
                break

    except KeyboardInterrupt:
        print("\nDeteniendo persistidor...")
    finally:
        productor_dlq.flush(5)
        consumidor.close()
        try:
            conn.close()
        except Exception:
            pass
        print("\n===== RESUMEN PERSISTIDOR =====")
        print(f"mensajes_kafka={msgs_kafka}  filas_upsert={filas_insertadas}  "
              f"lotes={lotes_confirmados}  dedup_en_lote={colisiones_lote}  dlq={enviados_dlq}")


if __name__ == "__main__":
    main()

"""Simulador de tramos de la red de agua (T1..T5).

Características (Nivel 5):
- Puede ejecutar un único tramo (--tramo T1, para contenedores individuales)
  o los 5 tramos a la vez (por defecto, para pruebas locales rápidas).
- Ondas sinusoidales coherentes en las uniones:
    Barrio A = a(t), Barrio B = b(t)
    T1 = a + b,  T2 = T4 = a,  T3 = T5 = b
- Cálculo de tick anclado al reloj (T0 = 2026-01-01 UTC) con bucle de ticks
  pendientes (tope de 60 s) y precisión de microsegundos enteros (ts_tick_us).
- Cierre limpio con SIGTERM (flush de Kafka en Docker) y partitioner murmur2_random.
"""
import argparse
import json
import math
import os
import random
import signal
import time
import uuid

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")
TOPIC = "lecturas"

T0 = int(float(os.getenv("T0", "1767225600")))
TPS_DEFAULT = float(os.getenv("TPS", "1.0"))

PRESION_BASE_TRAMO = {
    "T1": 4.2,
    "T2": 3.8,
    "T3": 3.6,
    "T4": 3.3,
    "T5": 3.1,
}


def al_entregar(err, msg):
    if err is not None:
        print(f"[ERROR] {msg.key().decode()} -> {err}")


def generar_lectura(tramo, tick, tps, periodo, fuga_tramo):
    """Genera caudal_entrada, caudal_salida y presion coherentes por tramo."""
    t_seg = tick / tps
    w = 2.0 * math.pi / periodo

    # Demanda sinusoidal de los dos barrios (desfasados entre sí)
    a = 30.0 + 7.5 * math.sin(w * t_seg)          # Barrio A (ramal T2 -> T4)
    b = 30.0 + 7.5 * math.sin(w * t_seg + 1.6)    # Barrio B (ramal T3 -> T5)

    base = {"T1": a + b, "T2": a, "T4": a, "T3": b, "T5": b}.get(tramo, a)
    caudal_entrada = round(base + random.uniform(-1.0, 1.0), 2)

    perdida = random.uniform(8.0, 12.0) if tramo == fuga_tramo else random.uniform(0.1, 0.4)
    caudal_salida = round(max(0.0, caudal_entrada - perdida), 2)

    p_base = PRESION_BASE_TRAMO.get(tramo, 3.5)
    presion = round(p_base - 0.015 * caudal_entrada + random.uniform(-0.05, 0.05), 2)

    return caudal_entrada, caudal_salida, presion


def main():
    signal.signal(signal.SIGTERM, signal.default_int_handler)

    parser = argparse.ArgumentParser(description="Simulador de tramos de red de agua")
    parser.add_argument(
        "--tramo",
        default=os.getenv("TRAMO", "T1,T2,T3,T4,T5"),
        help="Tramo(s) a simular: ej. 'T1' para 1 contenedor o 'T1,T2,T3,T4,T5'",
    )
    parser.add_argument("--tps", type=float, default=TPS_DEFAULT, help="Ticks por segundo (defecto: 1)")
    parser.add_argument("--periodo", type=float, default=60.0, help="Periodo de la onda en segundos (defecto: 60)")
    parser.add_argument("--fuga", default=os.getenv("FUGA_TRAMO", ""), help="Tramo con fuga simulada (ej: T3)")
    args = parser.parse_args()

    tramos = [t.strip() for t in args.tramo.split(",") if t.strip()]
    id_cliente = f"simulador-{'-'.join(tramos)}"

    productor = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": id_cliente,
        "acks": "all",
        "enable.idempotence": True,
        "partitioner": "murmur2_random",
        "delivery.timeout.ms": 120000,
        "linger.ms": 10,
    })

    boot_id = uuid.uuid4().hex[:8]
    seq = {t: 0 for t in tramos}
    max_atraso_ticks = max(1, int(60 * args.tps))

    print(f"Simulador activo · client.id={id_cliente} · boot_id={boot_id} · "
          f"tramos={tramos} · {args.tps} tps · onda={args.periodo}s")
    if args.fuga:
        print(f"ATENCIÓN: simulando fuga en {args.fuga}")

    ultimo = math.floor((time.time() - T0) * args.tps) - 1
    ultimo_log = time.monotonic()

    try:
        while True:
            actual = math.floor((time.time() - T0) * args.tps)
            if actual - ultimo > max_atraso_ticks:
                hueco = (actual - ultimo) - max_atraso_ticks
                print(f"[AVISO] Atraso de {actual - ultimo} ticks supera historial de 60 s; "
                      f"se omiten {hueco} ticks")
                ultimo = actual - max_atraso_ticks

            muestra_log = None
            for tick in range(ultimo + 1, actual + 1):
                ts_tick_us = T0 * 1_000_000 + round((tick * 1_000_000) / args.tps)
                ts_tick = ts_tick_us / 1_000_000
                ts_real = time.time()

                for tramo in tramos:
                    seq[tramo] += 1
                    q_in, q_out, presion = generar_lectura(
                        tramo, tick, args.tps, args.periodo, args.fuga
                    )
                    mensaje = {
                        "tramo": tramo,
                        "tick": tick,
                        "ts_tick": ts_tick,
                        "ts_tick_us": ts_tick_us,
                        "ts_real": ts_real,
                        "boot_id": boot_id,
                        "seq": seq[tramo],
                        "epoch": 0,
                        "lamport": 0,
                        "caudal_entrada": q_in,
                        "caudal_salida": q_out,
                        "presion": presion,
                    }
                    productor.produce(
                        TOPIC,
                        key=tramo,
                        value=json.dumps(mensaje),
                        on_delivery=al_entregar,
                    )
                    if tramo == tramos[0]:
                        muestra_log = (tramo, tick, q_in, q_out, presion)

            ultimo = max(ultimo, actual)
            productor.poll(0)

            if muestra_log and (time.monotonic() - ultimo_log >= 5.0):
                t_m, tk_m, qi_m, qo_m, p_m = muestra_log
                print(f"[tick={tk_m}] enviados={sum(seq.values())} "
                      f"{t_m}: Q_in={qi_m} Q_out={qo_m} P={p_m}")
                ultimo_log = time.monotonic()

            siguiente_seg = T0 + (ultimo + 1) / args.tps
            espera = siguiente_seg - time.time()
            if espera > 0:
                time.sleep(espera)

    except KeyboardInterrupt:
        print("\nDeteniendo simulador...")
    finally:
        pendientes = productor.flush(15)
        print(f"Simulador parado ({id_cliente}). Enviados={sum(seq.values())}, sin_entregar={pendientes}")


if __name__ == "__main__":
    main()

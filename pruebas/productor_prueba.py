"""Productor de prueba: simula N tramos publicando 1 mensaje por tramo y tick.

Configuración de producción decidida en v5: acks=all + idempotente.
Cada mensaje lleva (tramo, boot_id, seq) para que el consumidor detecte
pérdidas y duplicados.
"""
import argparse
import json
import os
import time
import uuid

from confluent_kafka import Producer

BOOTSTRAP = os.getenv("KAFKA_BOOTSTRAP", "localhost:9092,localhost:9094,localhost:9096")
TOPIC = "lecturas"

# Los callbacks se ejecutan dentro de poll()/flush(), en ESTE mismo hilo,
# así que estos contadores no necesitan lock.
contadores = {"enviados": 0, "confirmados": 0, "fallidos": 0}


def al_entregar(err, msg):
    if err is not None:
        contadores["fallidos"] += 1
        print(f"[ERROR] {msg.key().decode()} -> {err}")
    else:
        contadores["confirmados"] += 1


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--tramos", type=int, default=5)
    parser.add_argument("--tps", type=float, default=1.0, help="ticks por segundo")
    parser.add_argument("--duracion", type=float, default=120.0, help="segundos")
    args = parser.parse_args()

    productor = Producer({
        "bootstrap.servers": BOOTSTRAP,
        "client.id": "productor-prueba",
        "acks": "all",
        "enable.idempotence": True,
        "delivery.timeout.ms": 120000,
        "linger.ms": 10,
    })

    boot_id = uuid.uuid4().hex[:8]
    tramos = [f"T{i}" for i in range(1, args.tramos + 1)]
    seq = {t: 0 for t in tramos}
    periodo = 1.0 / args.tps
    print(f"boot_id={boot_id} · {len(tramos)} tramos · {args.tps} tps "
          f"= {len(tramos) * args.tps:.0f} msg/s · {args.duracion:.0f} s")

    inicio = time.monotonic()          # monotonic para medir tiempos (v6)
    siguiente = inicio
    ultimo_informe = inicio
    tick = 0
    try:
        while time.monotonic() - inicio < args.duracion:
            for tramo in tramos:
                seq[tramo] += 1
                mensaje = {
                    "tramo": tramo,
                    "tick": tick,
                    "ts_real": time.time(),  # reloj del contenedor (v6)
                    "boot_id": boot_id,
                    "seq": seq[tramo],
                }
                productor.produce(TOPIC, key=tramo, value=json.dumps(mensaje),
                                  on_delivery=al_entregar)
                contadores["enviados"] += 1
            productor.poll(0)          # atiende callbacks pendientes
            tick += 1

            ahora = time.monotonic()
            if ahora - ultimo_informe >= 5:
                pendientes = contadores["enviados"] - contadores["confirmados"] - contadores["fallidos"]
                print(f"[{ahora - inicio:6.1f}s] {contadores} pendientes={pendientes}")
                ultimo_informe = ahora

            siguiente += periodo
            espera = siguiente - time.monotonic()
            if espera > 0:
                time.sleep(espera)
    except KeyboardInterrupt:
        print("\nInterrumpido, vaciando cola...")
    finally:
        sin_entregar = productor.flush(130)
        print("\n===== RESUMEN PRODUCTOR =====")
        print(f"boot_id={boot_id}  {contadores}  sin_entregar={sin_entregar}")
        ok = contadores["fallidos"] == 0 and sin_entregar == 0
        print("RESULTADO:", "OK, todo confirmado" if ok else "HAY MENSAJES NO CONFIRMADOS")


if __name__ == "__main__":
    main()
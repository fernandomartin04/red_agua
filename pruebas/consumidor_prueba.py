"""Consumidor de prueba: commit manual DESPUÉS de procesar (v5).

Detecta huecos (pérdidas) y duplicados por (tramo, boot_id, seq).
"""
import argparse
import json
import os
import time
from collections import defaultdict

from confluent_kafka import Consumer, KafkaException, KafkaError

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


def al_asignar(consumidor, particiones):
    print("[REBALANCEO] asignadas:", sorted(p.partition for p in particiones))


def al_revocar(consumidor, particiones):
    print("[REBALANCEO] revocadas:", sorted(p.partition for p in particiones))


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("--grupo", default=f"prueba-{int(time.time())}")
    args = parser.parse_args()

    consumidor = Consumer({
        "bootstrap.servers": BOOTSTRAP,
        "group.id": args.grupo,
        "client.id": f"consumidor-prueba-{os.getpid()}",
        "enable.auto.commit": False,
        "auto.offset.reset": "earliest",
    })
    consumidor.subscribe([TOPIC], on_assign=al_asignar, on_revoke=al_revocar)
    print(f"Grupo: {args.grupo}  (Ctrl+C para terminar y ver el resumen)")

    vistos = defaultdict(set)          # (tramo, boot_id) -> {seq}
    recibidos = duplicados = 0
    ultimo_informe = time.monotonic()
    try:
        while True:
            msg = consumidor.poll(1.0)
            if msg is not None:
                if msg.error():
                    print("[ERROR]", msg.error())
                    continue
                datos = json.loads(msg.value())
                clave = (datos["tramo"], datos["boot_id"])
                recibidos += 1
                if datos["seq"] in vistos[clave]:
                    duplicados += 1
                else:
                    vistos[clave].add(datos["seq"])
                # Aquí iría "persistir en BD". Solo DESPUÉS se confirma el offset.
                try:
                    consumidor.commit(message=msg, asynchronous=False)
                except KafkaException as e:
                    err = e.args[0]
                    if err.code() in ERRORES_COMMIT_TEMPORALES or err.retriable():
                        print(f"[COMMIT] fallo temporal ({err.name()}), se sigue consumiendo")
                    else:
                        raise

            if time.monotonic() - ultimo_informe >= 5:
                print(f"recibidos={recibidos} duplicados={duplicados}")
                ultimo_informe = time.monotonic()
    except KeyboardInterrupt:
        pass
    finally:
        consumidor.close()             # sale del grupo limpiamente (rebalanceo rápido)
        print("\n===== RESUMEN CONSUMIDOR =====")
        huecos_totales = 0
        for (tramo, boot_id), seqs in sorted(vistos.items()):
            esperados = max(seqs) - min(seqs) + 1
            huecos = esperados - len(seqs)
            huecos_totales += huecos
            print(f"{tramo} boot={boot_id}: {len(seqs)} únicos "
                  f"(seq {min(seqs)}..{max(seqs)}) huecos={huecos}")
        print(f"recibidos={recibidos} duplicados={duplicados} huecos={huecos_totales}")
        print("RESULTADO:", "OK, sin pérdidas" if huecos_totales == 0 else "HAY PÉRDIDAS")


if __name__ == "__main__":
    main()
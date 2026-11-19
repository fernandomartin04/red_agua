"""Tests del persistidor y del simulador (pytest).

- Tests unitarios: no necesitan contenedores arrancados.
- Tests de BD: necesitan TimescaleDB en localhost:5432. Trabajan dentro de una
  transacción que se deshace al final (ROLLBACK), así que no dejan filas.

Ejecutar desde red_agua/:
    pruebas/.venv/bin/pytest -v tests/
"""
import sys
from datetime import datetime, timezone
from pathlib import Path

import psycopg2
import pytest
from psycopg2.extras import execute_values

BASE_DIR = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(BASE_DIR / "persistencia"))
sys.path.insert(0, str(BASE_DIR / "motor"))
import persistor  # noqa: E402
import simulador  # noqa: E402


# ---------------------------------------------------------------- unitarios
def test_mensaje_del_productor_de_prueba_requiere_modo_prueba():
    """Sin --modo-prueba un mensaje sin sensores se rechaza; con modo_prueba=True se acepta."""
    datos = {"tramo": "T1", "tick": 5, "ts_real": 1795000000.5, "boot_id": "abc", "seq": 6}
    with pytest.raises(ValueError):
        persistor.extraer_filas(datos, tps=10, modo_prueba=False)

    filas = persistor.extraer_filas(datos, tps=10, modo_prueba=True)
    assert len(filas) == 1
    ts_tick, _, tramo, sensor, valor, estado, tick, boot_id, seq, epoch, lamport = filas[0]
    assert (tramo, sensor, valor, estado, tick, boot_id, seq) == (
        "T1", "seq_prueba", 6.0, "OK", 5, "abc", 6
    )


def test_mensaje_con_varios_sensores_y_sin_dato():
    """Un sensor con 'sin_dato' produce valor=None y estado='SIN_DATO' sin descartar los demás."""
    datos = {
        "tramo": "T2",
        "tick": 10,
        "boot_id": "x",
        "seq": 1,
        "caudal_entrada": 50.0,
        "caudal_salida": "sin_dato",
        "presion": 3.2,
    }
    filas = persistor.extraer_filas(datos, tps=10)
    por_sensor = {f[3]: (f[4], f[5]) for f in filas}
    assert por_sensor["caudal_entrada"] == (50.0, "OK")
    assert por_sensor["caudal_salida"] == (None, "SIN_DATO")
    assert por_sensor["presion"] == (3.2, "OK")


def test_ts_tick_determinista_microsegundos():
    """Dos envíos del mismo tick producen exactamente el mismo ts_tick aunque ts_real cambie."""
    a = {"tramo": "T1", "tick": 100, "ts_real": 1795000010.0, "seq": 1, "presion": 3.5}
    b = {"tramo": "T1", "tick": 100, "ts_real": 1795000099.0, "seq": 1, "presion": 3.5}
    fa = persistor.extraer_filas(a, tps=10)[0]
    fb = persistor.extraer_filas(b, tps=10)[0]
    assert fa[0] == fb[0]
    assert fa[1] != fb[1]


def test_balance_ondas_simulador_coherente():
    """En el mismo tick, el caudal de T1 es aproximadamente la suma de T2 y T3."""
    q1, _, _ = simulador.generar_lectura("T1", tick=25, tps=1.0, periodo=60.0, fuga_tramo="")
    q2, _, _ = simulador.generar_lectura("T2", tick=25, tps=1.0, periodo=60.0, fuga_tramo="")
    q3, _, _ = simulador.generar_lectura("T3", tick=25, tps=1.0, periodo=60.0, fuga_tramo="")
    # Como cada tramo tiene ruido uniforme [-1, 1], la diferencia máxima es <= 3.0 m3/h
    assert abs(q1 - (q2 + q3)) <= 3.5


# ---------------------------------------------------------------- con BD
@pytest.fixture
def cur():
    try:
        conn = psycopg2.connect(
            host=persistor.PG_HOST,
            port=persistor.PG_PORT,
            dbname=persistor.PG_DB,
            user=persistor.PG_USER,
            password=persistor.PG_PASS,
            connect_timeout=3,
        )
    except psycopg2.OperationalError:
        pytest.skip("TimescaleDB no está arrancada")
    c = conn.cursor()
    yield c
    conn.rollback()
    conn.close()


def fila(tick, valor, tramo="TEST", epoch=0):
    ts = datetime(2000, 1, 1, tzinfo=timezone.utc).timestamp() + tick
    return (
        datetime.fromtimestamp(ts, tz=timezone.utc),
        datetime.now(timezone.utc),
        tramo,
        "presion",
        valor,
        "OK",
        tick,
        "test",
        tick + 1,
        epoch,
        0,
    )


def contar(cur):
    cur.execute("SELECT count(*) FROM lecturas WHERE tramo = 'TEST'")
    return cur.fetchone()[0]


def test_upsert_idempotente_y_fencing_epoch(cur):
    """Un reenvío no duplica filas y un epoch menor (zombi) no pisa a un epoch mayor."""
    lote_lider_nuevo = [fila(1, 3.5, epoch=5)]
    lote_zombi_viejo = [fila(1, 9.9, epoch=4)]
    execute_values(cur, persistor.SQL_UPSERT, lote_lider_nuevo)
    execute_values(cur, persistor.SQL_UPSERT, lote_zombi_viejo)
    assert contar(cur) == 1
    cur.execute("SELECT valor, epoch FROM lecturas WHERE tramo = 'TEST' AND tick = 1")
    assert cur.fetchone() == (3.5, 5)


def test_lote_con_clave_repetida_falla_sin_deduplicar(cur):
    """Demuestra por qué el persistidor deduplica en memoria antes del INSERT."""
    lote = [fila(1, 1.0), fila(1, 2.0)]
    with pytest.raises(psycopg2.errors.CardinalityViolation):
        execute_values(cur, persistor.SQL_UPSERT, lote)


def test_lote_deduplicado_con_diccionario(cur):
    """Con el diccionario por (ts_tick, tramo, sensor) se queda la última y no falla."""
    lote = [fila(1, 1.0), fila(2, 5.0), fila(1, 2.0)]
    dedup = {}
    for f in lote:
        dedup[(f[0], f[2], f[3])] = f
    execute_values(cur, persistor.SQL_UPSERT, list(dedup.values()))
    assert contar(cur) == 2
    cur.execute("SELECT valor FROM lecturas WHERE tramo = 'TEST' AND tick = 1")
    assert cur.fetchone()[0] == 2.0

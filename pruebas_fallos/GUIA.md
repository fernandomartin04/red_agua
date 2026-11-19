# Pruebas de fallos de la tubería Kafka → persistidor → TimescaleDB

Todo se lanza desde `red_agua/`. Antes de nada, en **cada terminal**:

```bash
cd ~/Escritorio/sistemas_distribuidos/red_agua
source pruebas/.venv/bin/activate
```

## Esquema común (3 terminales)

| Terminal | Qué hace |
|---|---|
| T1 | Productor de prueba (`pruebas/productor_prueba.py`) |
| T2 | Persistidor (`persistencia/persistor.py`) |
| T3 | Script de la caída + verificación |

Pasos de cada prueba:

1. **T3** `./pruebas_fallos/preparar.sh` → al final te da un nombre de grupo (ej. `persistor-162057`). Úsalo en toda la prueba.
2. **T2** `python persistencia/persistor.py --grupo <GRUPO> --modo-prueba`
   (la primera vez se traga el histórico del topic, unos 60 000 mensajes en ~20 s; espera a que se calme).
3. **T1** `python pruebas/productor_prueba.py --tps 10 --duracion 150`
4. **T3** el script de la prueba (inmediatamente después del productor).
5. Cuando el productor imprima el RESUMEN, espera ~10 s, para el persistidor con Ctrl+C y en **T3**:
   `./pruebas_fallos/verificar_bd.sh "" <confirmados_del_productor>`

`./pruebas_fallos/estado.sh <GRUPO>` saca una foto (contenedores, ISR, lag, filas) cuando quieras.

---

## Prueba D · Caída de TimescaleDB
```bash
./pruebas_fallos/caida_timescale.sh <GRUPO>
```
- 30 s: `docker kill timescaledb` · 60 s: `docker start` · 80 s: lag final.
- Capturas: T2 con `[BD] Error conectando... reintentando` y luego `Conectado`; T3 con el lag creciendo y luego a 0.
- Por qué funciona: si la BD no confirma el lote, no se hace commit en Kafka. Kafka hace de buffer.

## Prueba E · kill -9 del persistidor
```bash
./pruebas_fallos/caida_persistor.sh <GRUPO>
```
- 30 s: `kill -9` al persistidor. A los 40 s el script te avisa: **relánzalo en T2 con el MISMO grupo**.
- Puede tardar hasta ~45 s en asignarse particiones: el broker espera a que caduque la sesión del
  consumidor muerto (`session.timeout.ms` = 45 s), porque con kill -9 no se despidió del grupo.
- Por qué funciona: retoma desde el último offset confirmado. El último lote se reprocesa y el UPSERT lo absorbe.

## Prueba F · Caída del coordinador del grupo del persistidor
```bash
./pruebas_fallos/caida_coordinador.sh <GRUPO>
```
- 30 s: busca el coordinador y le hace `docker kill` · 60 s: lo arranca · estado final con ISR y lag.
- Es la prueba C, pero ahora con commit en BD + commit en Kafka.

## Prueba G · Reinicio completo (docker compose down / up)
Con productor y persistidor **parados**:
```bash
./pruebas_fallos/reinicio_total.sh <GRUPO>
```
- Cuenta mensajes del topic y filas de la BD, hace `down` (sin `-v`), `up`, y vuelve a contar.
- Por qué funciona: los contenedores se destruyen pero los volúmenes no.

## Tests automáticos (pytest)
```bash
pytest -v tests/
```
6 tests: formato del mensaje, `ts_tick` determinista, UPSERT idempotente, el error de Postgres
con una clave repetida en el mismo lote y cómo lo evita el diccionario.

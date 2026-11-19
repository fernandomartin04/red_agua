#!/bin/bash
# Comprueba en TimescaleDB que no se ha perdido ni duplicado nada.
# Uso: ./verificar_bd.sh [boot_id] [confirmados_productor]
#   - Sin boot_id coge el de la última ejecución del productor.
#   - Si pasas los confirmados del productor, compara también el total.
source "$(dirname "$0")/comun.sh"

BOOT=${1:-$($PSQL -tAc "SELECT boot_id FROM lecturas ORDER BY ts_real DESC LIMIT 1")}
ESPERADOS=$2
if [ -z "$BOOT" ]; then echo "La tabla lecturas está vacía"; exit 1; fi

paso "Verificación en TimescaleDB · boot_id=$BOOT"
# seq va de 1 a N por tramo: si hay N filas y el máximo es N, no falta ninguna
$PSQL -c "
SELECT tramo,
       count(*)                      AS filas,
       min(seq)                      AS seq_min,
       max(seq)                      AS seq_max,
       max(seq) - count(*)           AS huecos,
       count(*) - count(DISTINCT seq) AS duplicados
FROM lecturas WHERE boot_id = '$BOOT'
GROUP BY tramo ORDER BY tramo;"

read -r FILAS HUECOS DUPS <<< "$($PSQL -tA -F' ' -c "
SELECT sum(filas), sum(huecos), sum(dups) FROM (
  SELECT count(*) AS filas, max(seq) - count(*) AS huecos,
         count(*) - count(DISTINCT seq) AS dups
  FROM lecturas WHERE boot_id = '$BOOT' GROUP BY tramo) t;")"

echo "TOTAL filas=$FILAS  huecos=$HUECOS  duplicados=$DUPS"
if [ -n "$ESPERADOS" ]; then
    echo "Confirmados por el productor=$ESPERADOS  ->  diferencia=$(( ESPERADOS - FILAS ))"
fi

if [ "$HUECOS" = "0" ] && [ "$DUPS" = "0" ] && { [ -z "$ESPERADOS" ] || [ "$ESPERADOS" = "$FILAS" ]; }; then
    echo "RESULTADO: OK, 0 pérdidas y 0 duplicados en la BD"
else
    echo "RESULTADO: HAY DIFERENCIAS"
fi

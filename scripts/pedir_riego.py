"""
Publica una SOLICITUD_RIEGO en wm.requests y muestra los avisos que CENTRAL
devuelve a ese operario, hasta el RESUMEN o la DENEGADA.

Sirve para probar D5 sin el WM_FO de Rubén. Pone el ts actual él solo
(CENTRAL descarta las peticiones con más de 30 s).

Uso (desde la raíz del repo):
    python -m scripts.pedir_riego localhost:9092 WS-04 20 --fo FO-12
"""

import argparse
import sys
import time
import uuid

from common import kafka_utils as ku
from common import mensajes as m

p = argparse.ArgumentParser()
p.add_argument("broker")
p.add_argument("ws_id")
p.add_argument("duracion_s", type=int)
p.add_argument("--fo", default="FO-12")
p.add_argument("--espera", type=int, default=120, help="segundos máximos")
args = p.parse_args()

# Primero el consumidor, y esperamos a que tenga particiones: con "latest"
# solo ve lo que llega después, y no queremos perder el primer PASO.
consumer = ku.crear_consumer(args.broker, f"prueba-fo-{uuid.uuid4()}",
                             [m.T_NOTIFICATIONS])
limite = time.time() + 15
while not consumer.assignment():
    if time.time() > limite:
        sys.exit("Sin particiones en 15 s: ¿Kafka arrancado y topics creados?")
    consumer.poll(0.5)

req_id = str(uuid.uuid4())
peticion = m.solicitud_riego(req_id, "FO", args.fo, args.ws_id, args.duracion_s)
ku.enviar(ku.crear_producer(args.broker), m.T_REQUESTS, args.ws_id, peticion)
print(f"Enviada: {args.fo} pide {args.ws_id} durante {args.duracion_s} s "
      f"(req_id {req_id[:8]})")

limite = time.time() + args.espera
while time.time() < limite:
    msg = consumer.poll(1.0)
    if msg is None or msg.error() or not ku.es_para_mi(msg, args.fo):
        continue
    d = ku.leer(msg)
    if d.get("req_id") != req_id:
        continue
    tipo = d["tipo"]
    if tipo == "PASO":
        print("  PASO     ", d["texto"])
    elif tipo == "TELEMETRIA":
        print(f"  TELEMETRÍA  {d['caudal_lpm']} L/min · {d['volumen_l']} L")
    else:
        print(f"  {tipo:<10}", {k: v for k, v in d.items()
                                if k not in ("tipo", "ts", "req_id", "fo_id")})
    if tipo in ("RESUMEN", "DENEGADA"):
        break
consumer.close()

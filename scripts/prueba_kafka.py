"""
Prueba de D1 con el Kafka local: envía un mensaje y lo recibe.

Requisitos: Kafka levantado (docker compose up -d) y los topics creados.
Ejecutar desde la raíz del repo:

    python -m scripts.prueba_kafka              (usa localhost:9092)
    python -m scripts.prueba_kafka ip:puerto
"""

import sys
import threading
import time
import uuid

from common import kafka_utils as ku
from common import mensajes as m

broker = sys.argv[1] if len(sys.argv) > 1 else "localhost:9092"
mi_id = "WS-04"

# Un group.id nuevo en cada ejecución, para que use "latest" desde cero.
grupo = f"prueba-{uuid.uuid4()}"
consumer = ku.crear_consumer(broker, grupo, [m.T_TELEMETRY])

# Con "latest", el consumidor solo ve lo que llega DESPUÉS de que Kafka le
# asigne las particiones. Si enviáramos antes, el mensaje se perdería.
# Por eso hacemos poll() hasta que tenga particiones asignadas.
print("Esperando a que Kafka asigne particiones...")
limite = time.time() + 15
while not consumer.assignment():
    if time.time() > limite:
        sys.exit("ERROR: sin particiones en 15 s. ¿Kafka arrancado y topics creados?")
    consumer.poll(0.5)
print("Particiones asignadas:", [p.partition for p in consumer.assignment()])

# Enviamos un STARTED, como haría un Engine.
producer = ku.crear_producer(broker)
req_id = str(uuid.uuid4())
enviado = m.started(req_id, mi_id)
ku.enviar(producer, m.T_TELEMETRY, mi_id, enviado)
print("Enviado: ", enviado)

# Lo leemos. Puede haber mensajes de otros (otras claves o del compañero):
# solo nos vale el nuestro.
limite = time.time() + 15
recibido = None
while time.time() < limite:
    msg = consumer.poll(1.0)
    if msg is None or msg.error():
        continue
    if not ku.es_para_mi(msg, mi_id):
        continue
    datos = ku.leer(msg)
    if datos.get("req_id") == req_id:
        recibido = datos
        break

consumer.close()

if recibido == enviado:
    print("Recibido:", recibido)
    print("OK: el mensaje ha ido y vuelto por Kafka.")
else:
    sys.exit("ERROR: no ha llegado el mensaje en 15 s.")

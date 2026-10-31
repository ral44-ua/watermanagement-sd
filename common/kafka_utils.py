"""
Utilidades Kafka comunes (confluent-kafka).

Todos los módulos (CENTRAL, Engine, FO) envían y reciben a través de estas
funciones, para que el formato sea siempre el mismo: JSON en UTF-8 y la
clave (key) como texto.

Recordatorio del contrato:
  - Clave = ws_id ("WS-04") en requests, commands y telemetry;
    fo_id ("FO-12") en notifications; "red" en status. Los IDs llevan guion.
  - Kafka manda todos los mensajes con la misma clave a la misma partición,
    y dentro de una partición el orden está garantizado: nunca llegará un
    FINISHED antes que el STARTED de la misma estación.
  - Un group.id distinto por Engine/FO (wm-engine-<ID>, wm-fo-<ID>) =
    modelo publicación/suscripción: cada uno recibe todos los mensajes del
    topic y se queda con los suyos usando es_para_mi().
  - CENTRAL usa el group.id fijo "wm-central".
"""

import json

from confluent_kafka import Consumer, KafkaError, KafkaException, Producer
from confluent_kafka.admin import AdminClient, NewTopic


def crear_producer(broker: str) -> Producer:
    """Crea un productor. broker = "ip:puerto", por ejemplo "localhost:9092".

    Un mismo Producer se puede compartir entre hilos.
    """
    return Producer({"bootstrap.servers": broker})


def crear_topics(broker: str, topics: list, particiones: int = 3,
                 espera_s: float = 10) -> list:
    """Crea en el bróker los topics que falten. Devuelve los que ha creado.

    Por qué: si un consumidor se suscribe a un topic que todavía no existe,
    Kafka no lo crea (los consumidores no crean topics) y el consumidor no lo
    ve hasta que vuelve a pedir la lista de topics, cada 5 minutos. En un Kafka
    recién desplegado (por ejemplo, en Railway) CENTRAL estaría ese rato sin
    atender peticiones. CENTRAL llama a esta función al arrancar.

    replication_factor=1: solo hay un bróker, no puede haber más copias.
    Si otro programa crea el mismo topic a la vez, Kafka responde
    TOPIC_ALREADY_EXISTS: no es un error, el topic ya está.
    """
    admin = AdminClient({"bootstrap.servers": broker})
    existentes = admin.list_topics(timeout=espera_s).topics
    nuevos = [NewTopic(t, num_partitions=particiones, replication_factor=1)
              for t in topics if t not in existentes]
    creados = []
    if not nuevos:
        return creados
    for topic, futuro in admin.create_topics(nuevos, request_timeout=espera_s).items():
        try:
            futuro.result()          # espera a que el bróker conteste
            creados.append(topic)
        except KafkaException as ex:
            if ex.args[0].code() != KafkaError.TOPIC_ALREADY_EXISTS:
                raise
    return creados


def crear_consumer(broker: str, group_id: str, topics: list,
                   offset: str = "latest", extra: dict = None) -> Consumer:
    """Crea un consumidor ya suscrito a la lista de topics.

    offset (auto.offset.reset) solo se aplica si el grupo no tiene un offset
    guardado:
      - "latest" (Engine y FO): solo lo nuevo. Con "earliest", un Engine
        reiniciado releería órdenes START antiguas y empezaría a regar solo.
      - "earliest": desde el principio del log.
    El commit de offsets es automático (opción por defecto de Kafka).

    session.timeout.ms = 10 s (por defecto Kafka usa 45 s): es lo que tarda
    Kafka en dar por muerto a un consumidor que deja de dar señales. Si un
    programa se cierra de golpe (cerrar la ventana, docker kill) no se despide
    del grupo, y al relanzarlo con el mismo ID su consumidor nuevo no recibe
    particiones hasta que Kafka echa al viejo. Con 45 s, el FO relanzado se
    rendía antes ("Kafka no responde", espera 30 s) y el Engine relanzado
    tardaba 45 s en volver a recibir órdenes. Con 10 s vuelven enseguida.
    (Mientras siga vivo, el consumidor manda un latido cada 3 s.)

    extra: más opciones de configuración, que sustituyen a las de aquí.

    Un Consumer NO se puede compartir entre hilos: uno por hilo.
    """
    config = {
        "bootstrap.servers": broker,
        "group.id": group_id,
        "auto.offset.reset": offset,
        "session.timeout.ms": 10000,
    }
    config.update(extra or {})
    consumer = Consumer(config)
    consumer.subscribe(topics)
    return consumer


def enviar(producer: Producer, topic: str, key: str, mensaje: dict,
           esperar: bool = True) -> None:
    """Serializa el dict a JSON UTF-8 y lo publica con su clave.

    ensure_ascii=False deja las tildes tal cual ("Comprobando estado…")
    en vez de escribirlas como \\u00f3; el texto se codifica en UTF-8.

    produce() solo deja el mensaje en una cola interna; la librería lo
    manda al bróker por detrás.
      - esperar=True (Engine, FO): flush() espera a que el bróker confirme.
        Si la función vuelve, el mensaje ha salido.
      - esperar=False (CENTRAL): no espera. CENTRAL envía desde los hilos
        de los sockets, y si Kafka tardara, esos hilos dejarían de responder
        a los Monitores (ALIVE, LEAK...). poll(0) atiende los avisos
        pendientes de la librería sin bloquear.
    """
    valor = json.dumps(mensaje, ensure_ascii=False).encode("utf-8")
    producer.produce(topic, key=key.encode("utf-8"), value=valor)
    if esperar:
        producer.flush()
    else:
        producer.poll(0)


def leer(msg) -> dict:
    """Convierte el valor de un mensaje recibido (bytes JSON) en un dict."""
    return json.loads(msg.value().decode("utf-8"))


def es_para_mi(msg, mi_id: str) -> bool:
    """True si la clave del mensaje es mi ID ("WS-04", "FO-12"...).

    Se comprueba que la clave no sea None antes de hacer .decode(): un
    mensaje enviado sin clave (por ejemplo, a mano desde la consola) haría
    saltar un AttributeError y tiraría todo el hilo consumidor.
    """
    key = msg.key()
    if key is None:
        return False
    return key.decode("utf-8") == mi_id

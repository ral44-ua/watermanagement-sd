"""
Tests de common/kafka_utils.crear_topics sin bróker: se sustituye AdminClient
por uno falso (monkeypatch cambia una variable de un módulo solo durante el
test).

Ejecutar desde la raíz:  python -m pytest tests/test_kafka_utils.py
"""

from concurrent.futures import Future

import pytest
from confluent_kafka import KafkaError, KafkaException

from common import kafka_utils as ku
from common import mensajes as m


class AdminFalso:
    """Imita lo que usa crear_topics: list_topics() y create_topics()."""
    existentes = set()
    ya_creado_por_otro = set()       # topics que "otro" crea a la vez
    pedidos = []                     # (topic, particiones, réplicas) pedidos

    def __init__(self, config):
        pass

    def list_topics(self, timeout=None):
        class Metadatos:
            topics = {t: None for t in AdminFalso.existentes}
        return Metadatos()

    def create_topics(self, nuevos, request_timeout=None):
        resultado = {}
        for n in nuevos:
            AdminFalso.pedidos.append((n.topic, n.num_partitions, n.replication_factor))
            f = Future()
            if n.topic in AdminFalso.ya_creado_por_otro:
                f.set_exception(KafkaException(KafkaError(KafkaError.TOPIC_ALREADY_EXISTS)))
            else:
                f.set_result(None)
            resultado[n.topic] = f
        return resultado


@pytest.fixture
def admin(monkeypatch):
    AdminFalso.existentes = set()
    AdminFalso.ya_creado_por_otro = set()
    AdminFalso.pedidos = []
    monkeypatch.setattr(ku, "AdminClient", AdminFalso)
    return AdminFalso


def test_crea_los_que_faltan_con_3_particiones(admin):
    admin.existentes = {m.T_REQUESTS, m.T_STATUS}
    creados = ku.crear_topics("falso:9092", m.TOPICS)
    assert creados == [m.T_COMMANDS, m.T_TELEMETRY, m.T_NOTIFICATIONS]
    assert all(p == 3 and r == 1 for _, p, r in admin.pedidos)


def test_si_ya_existen_no_hace_nada(admin):
    admin.existentes = set(m.TOPICS)
    assert ku.crear_topics("falso:9092", m.TOPICS) == []
    assert admin.pedidos == []


def test_ya_existe_a_la_vez_no_es_error(admin):
    admin.ya_creado_por_otro = {m.T_REQUESTS}
    creados = ku.crear_topics("falso:9092", m.TOPICS)
    assert m.T_REQUESTS not in creados and len(creados) == 4

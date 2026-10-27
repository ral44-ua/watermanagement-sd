"""Tests del protocolo de tramas.

socket.socketpair() crea dos sockets ya conectados entre sí dentro del mismo
proceso: lo que se escribe en uno se lee en el otro. Así probamos el protocolo
sin abrir puertos ni lanzar CENTRAL.

Como enviar_trama() se bloquea esperando el ACK, el "otro extremo" tiene que
ejecutarse en un hilo aparte; si no, el test se quedaría colgado.
"""
import socket
import threading

import pytest

from common.protocol import (
    ACK, ETX, NACK, STX,
    empaquetar, enviar_trama, leer_byte, lrc, peticion, recibir_trama,
)


def par_sockets():
    a, b = socket.socketpair()
    a.settimeout(2)  # si algo falla, el test acaba con error en vez de colgarse
    b.settimeout(2)
    return a, b


def test_lrc_de_ok():
    assert lrc(b"OK") == b"\x04"


def test_empaquetar_ok():
    assert empaquetar("OK") == bytes([0x02, 0x4F, 0x4B, 0x03, 0x04])


def test_peticion_y_respuesta():
    cliente, servidor = par_sockets()

    def falso_servidor():
        texto = recibir_trama(servidor)               # "AUTH#WS-04#River Park"
        codigo = texto.split("#")[0]
        enviar_trama(servidor, f"OK#{codigo}")

    hilo = threading.Thread(target=falso_servidor)
    hilo.start()
    campos = peticion(cliente, "AUTH#WS-04#River Park")
    hilo.join(timeout=2)

    assert campos == ["OK", "AUTH"]


def test_lrc_corrupto_provoca_nack_y_luego_acepta_la_buena():
    emisor, receptor = par_sockets()
    resultado = {}

    hilo = threading.Thread(target=lambda: resultado.update(texto=recibir_trama(receptor)))
    hilo.start()

    emisor.sendall(STX + b"OK" + ETX + b"\x00")   # LRC mal (debería ser 0x04)
    assert leer_byte(emisor) == NACK

    emisor.sendall(empaquetar("OK"))              # retransmisión correcta
    assert leer_byte(emisor) == ACK

    hilo.join(timeout=2)
    assert resultado["texto"] == "OK"


def test_enviar_trama_se_rinde_tras_3_nack():
    emisor, receptor = par_sockets()

    def siempre_nack():
        for _ in range(3):
            for _ in range(len(empaquetar("HOLA"))):  # leer la trama entera
                leer_byte(receptor)
            receptor.sendall(NACK)

    hilo = threading.Thread(target=siempre_nack)
    hilo.start()
    with pytest.raises(IOError):
        enviar_trama(emisor, "HOLA")
    hilo.join(timeout=2)


def test_socket_cerrado_lanza_connection_error():
    a, b = par_sockets()
    b.close()
    with pytest.raises(ConnectionError):
        leer_byte(a)

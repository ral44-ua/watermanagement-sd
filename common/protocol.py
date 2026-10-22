"""
Protocolo de tramas para los sockets de WaterManagement.

Formato de una trama:  <STX> DATA <ETX> <LRC>
  - STX (0x02) marca el inicio y ETX (0x03) el final del mensaje.
  - DATA es texto UTF-8 con los campos separados por '#': "AUTH#WS-04#River Park".
  - LRC es 1 byte: el XOR de todos los bytes de DATA. Sirve para detectar corrupción.

Por qué hace falta: TCP entrega un chorro continuo de bytes, no "mensajes".
Un recv() puede devolver medio mensaje o dos pegados. STX/ETX nos dicen dónde
empieza y acaba cada uno, y el LRC si ha llegado bien.

Quien recibe una trama responde ACK si el LRC cuadra o NACK si no; tras un NACK
el emisor reenvía (máximo 3 intentos).

Se usa en dos conexiones persistentes:
  - Monitor (cliente) -> CENTRAL (servidor): AUTH, ALIVE, LEAK, RECOVERED.
  - Engine (cliente)  -> Monitor (servidor): HELLO, HEALTH.
"""

# Bytes de control. Son 'bytes' (no int) para poder concatenarlos y compararlos
# directamente con lo que devuelve sock.recv().
STX = b"\x02"   # Start of TeXt: empieza el mensaje
ETX = b"\x03"   # End of TeXt: acaba el mensaje
EOT = b"\x04"   # End Of Transmission: cierre ordenado de la conexión
ENQ = b"\x05"   # ENQuiry: "¿estás ahí?" al conectar
ACK = b"\x06"   # ACKnowledge: "recibido y LRC correcto"
NACK = b"\x15"  # Negative ACK: "LRC incorrecto, repite"

SEPARADOR = "#"


def lrc(data: bytes) -> bytes:
    """XOR de todos los bytes de DATA. Devuelve 1 byte.

    Ejemplo: "OK" -> 0x4F ^ 0x4B = 0x04.
    Si un bit cambia por el camino, el XOR sale distinto y el receptor lo nota.
    """
    r = 0
    for b in data:          # al iterar sobre 'bytes' cada b ya es un int (0-255)
        r ^= b
    return bytes([r])       # lo volvemos a convertir en un objeto bytes de longitud 1


def empaquetar(texto: str) -> bytes:
    """Convierte un texto en una trama lista para enviar: STX + DATA + ETX + LRC."""
    data = texto.encode("utf-8")
    return STX + data + ETX + lrc(data)


def leer_byte(sock) -> bytes:
    """Lee exactamente 1 byte del socket.

    recv() devuelve b"" cuando el otro extremo ha cerrado la conexión. Si no lo
    comprobáramos, los bucles de lectura girarían para siempre recibiendo b"".
    Lanzamos ConnectionError para que quien nos llame (Monitor, CENTRAL) sepa
    que la otra parte se ha ido.

    Si el socket tiene settimeout(), recv() lanza TimeoutError al agotarse:
    el Monitor usa eso para detectar un Engine que no contesta.
    """
    b = sock.recv(1)
    if not b:
        raise ConnectionError("socket cerrado por el otro extremo")
    return b


def recibir_trama(sock) -> str:
    """Lee una trama completa, comprueba el LRC y responde ACK o NACK.

    Devuelve el texto de DATA. Si el LRC falla, responde NACK y se queda
    esperando la retransmisión (por eso el 'while True').

    OJO: descarta todo lo que llegue antes de un STX, incluido un ENQ suelto.
    Por eso el servidor lee el ENQ inicial con leer_byte() antes de llamar aquí.
    """
    while True:
        # 1) Saltar basura hasta encontrar el inicio de trama.
        while leer_byte(sock) != STX:
            pass

        # 2) Acumular DATA hasta el ETX.
        data = b""
        while (b := leer_byte(sock)) != ETX:
            data += b

        # 3) El byte siguiente al ETX es el LRC: comprobarlo.
        if leer_byte(sock) == lrc(data):
            sock.sendall(ACK)
            return data.decode("utf-8")

        sock.sendall(NACK)  # el emisor reenviará la trama


def enviar_trama(sock, texto: str, reintentos: int = 3) -> None:
    """Envía una trama y espera ACK. Si llega NACK (o cualquier otra cosa), reenvía.

    Tras 'reintentos' fallos lanza IOError: no tiene sentido insistir para siempre.
    sendall() (y no send()) garantiza que se mandan todos los bytes.
    """
    for _ in range(reintentos):
        sock.sendall(empaquetar(texto))
        if leer_byte(sock) == ACK:
            return
    raise IOError(f"Sin ACK tras {reintentos} intentos: {texto}")


def peticion(sock, texto: str) -> list[str]:
    """Petición/respuesta completa: envía 'texto' y devuelve la respuesta en campos.

    Ejemplo: peticion(sock, "AUTH#WS-04#River Park") -> ["OK", "AUTH"]
    """
    enviar_trama(sock, texto)
    return recibir_trama(sock).split(SEPARADOR)

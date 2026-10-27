"""
CENTRAL falso para probar el Monitor sin el WM_Central de David.

    python -m tests.fake_central 9000              # contesta OK#<CODIGO> a todo
    python -m tests.fake_central 9000 --rechazar   # contesta KO al AUTH (prueba el error)

Acepta varios Monitor a la vez (un hilo por conexión) y muestra cada trama que recibe.
No guarda estado ni vigila los ALIVE: eso es trabajo del CENTRAL real.
"""
import argparse
import socket
import threading
from datetime import datetime

from common.protocol import ACK, ENQ, enviar_trama, leer_byte, recibir_trama
from ws.WM_WS_M import crear_servidor


def log(texto: str) -> None:
    print(f"[{datetime.now():%H:%M:%S}] {texto}", flush=True)


def atender(conn: socket.socket, addr, rechazar: bool) -> None:
    quien = f"{addr[0]}:{addr[1]}"
    log(f"[+] Conexión de {quien}")
    try:
        # Igual que hará el CENTRAL real: el ENQ se lee aparte con leer_byte(),
        # porque recibir_trama() lo descartaría.
        if leer_byte(conn) != ENQ:
            log(f"    {quien} no empezó con ENQ: cierro")
            return
        conn.sendall(ACK)
        log(f"    {quien}  ENQ -> ACK")

        while True:
            texto = recibir_trama(conn)
            codigo = texto.split("#")[0]
            if codigo == "AUTH" and rechazar:
                respuesta = "KO#AUTH#Rechazo de prueba"
            else:
                respuesta = f"OK#{codigo}"
            enviar_trama(conn, respuesta)
            log(f"    {quien}  {texto}  ->  {respuesta}")
    except OSError as e:
        log(f"[-] {quien} desconectado ({e})")
    finally:
        conn.close()


def main() -> None:
    parser = argparse.ArgumentParser(description="CENTRAL falso para pruebas")
    parser.add_argument("puerto", type=int)
    parser.add_argument("--rechazar", action="store_true", help="responde KO a los AUTH")
    args = parser.parse_args()

    try:
        srv = crear_servidor(args.puerto)     # el mismo que usa el Monitor
    except OSError as e:
        log(f"[ERROR] No puedo usar el puerto {args.puerto} ({e}). ¿Hay otro programa usándolo?")
        return
    srv.listen()
    srv.settimeout(1)                 # para que Ctrl+C funcione también en Windows
    log(f"CENTRAL falso escuchando en el puerto {args.puerto}")
    try:
        while True:
            try:
                conn, addr = srv.accept()
            except socket.timeout:       # vale en Python 3.9 y en 3.10+
                continue
            threading.Thread(target=atender, args=(conn, addr, args.rechazar), daemon=True).start()
    except KeyboardInterrupt:
        log("Cerrando CENTRAL falso")
    finally:
        srv.close()


if __name__ == "__main__":
    main()

"""
Monitor FALSO para probar CENTRAL sin el WM_WS_M de Rubén.

Hace lo mínimo que hace un Monitor frente a CENTRAL:
  ENQ -> ACK, AUTH, ALIVE cada 2 s, y con la tecla k alterna LEAK / RECOVERED.

Uso (desde la raíz del repo, con CENTRAL arrancada):
    python -m scripts.monitor_falso localhost:9000 WS-04 --ubicacion "River Park"

Teclas: k + Enter = fuga / reparada · q + Enter = salir (CENTRAL ve el
socket cerrado y pasa la WS a DESCONECTADA).
"""

import argparse
import socket
import threading
import time

from common.protocol import ACK, ENQ, EOT, peticion

p = argparse.ArgumentParser()
p.add_argument("central", help="ip:puerto de WM_Central")
p.add_argument("ws_id")
p.add_argument("--ubicacion", default="Parque de pruebas")
args = p.parse_args()

ip, puerto = args.central.split(":")
sock = socket.create_connection((ip, int(puerto)))
sock.sendall(ENQ)
if sock.recv(1) != ACK:
    raise SystemExit("CENTRAL no ha respondido ACK al ENQ")

# Dos hilos escriben en el mismo socket (ALIVE y el teclado): con un lock,
# cada petición (trama + respuesta) se hace entera sin mezclarse.
lock = threading.Lock()

with lock:
    respuesta = peticion(sock, f"AUTH#{args.ws_id}#{args.ubicacion}")
print("AUTH ->", "#".join(respuesta))
if respuesta[0] != "OK":
    raise SystemExit(1)


def alive():
    while True:
        time.sleep(2)
        try:
            with lock:
                peticion(sock, f"ALIVE#{args.ws_id}")
        except OSError:
            return


threading.Thread(target=alive, daemon=True).start()

fuga = False
print("k + Enter: fuga/reparada · q + Enter: salir")
while True:
    tecla = input().strip().lower()
    if tecla == "q":
        sock.sendall(EOT)
        sock.close()
        break
    if tecla == "k":
        fuga = not fuga
        texto = (f"LEAK#{args.ws_id}#Engine KO" if fuga
                 else f"RECOVERED#{args.ws_id}")
        with lock:
            print(texto, "->", "#".join(peticion(sock, texto)))

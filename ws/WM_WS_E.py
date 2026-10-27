"""
WM_WS_E · Engine de una Watering Station (WS).

Uso (siempre desde la raíz del repositorio):
    python -m ws.WM_WS_E <ip_broker:puerto> <ip_monitor:puerto>
    python -m ws.WM_WS_E 127.0.0.1:9092 127.0.0.1:9100

Orden de arranque: primero el Monitor (WM_WS_M) y después el Engine.

Qué hace (R3, solo sockets):
  1. Se conecta a su Monitor:  ENQ -> ACK,  HELLO -> ID#WS-04.
     El Engine no sabe su ID: se lo da el Monitor, que ya lo validó con CENTRAL.
  2. Contesta a cada HEALTH del Monitor con OK#HEALTH o KO#HEALTH.
  3. La tecla 'k' (+ Enter) alterna entre OK y KO para simular una avería:
     en KO el Monitor avisa a CENTRAL con LEAK; al volver a OK, con RECOVERED.
  4. Si pierde al Monitor, reintenta conectarse cada 3 s (el Monitor puede reiniciarse).

El broker de Kafka se recibe ya por parámetro (lo pide el enunciado), pero no se usa
hasta R4 (riego y telemetría).

Hilos:
  - Principal: menú de teclado con input().
  - hilo_salud: conexión con el Monitor y respuestas a HEALTH.
Los dos tocan 'estado_ok' (uno lo cambia, el otro lo lee), por eso va con 'lock'.
"""
import argparse
import socket
import threading
import time

from common.protocol import ACK, ENQ, enviar_trama, leer_byte, peticion, recibir_trama
from ws.WM_WS_M import ip_puerto, log      # mismas utilidades que el Monitor

TIMEOUT_MONITOR = 5      # s sin recibir HEALTH (llega cada 1 s) = Monitor perdido
ESPERA_RECONEXION = 3    # s entre intentos de conexión con el Monitor


class Engine:
    def __init__(self, dir_monitor: tuple[str, int], dir_broker: tuple[str, int]):
        self.dir_monitor = dir_monitor
        self.dir_broker = dir_broker          # se usará en R4 para Kafka
        self.ws_id = None                     # lo asigna el Monitor en el HELLO

        # estado_ok lo cambia el hilo del teclado y lo lee el hilo de salud.
        # Hoy es un solo bool, pero en R4 la tecla 'k' además cortará el riego:
        # varias cosas que deben pasar juntas, y para eso hace falta el lock.
        self.lock = threading.Lock()
        self.estado_ok = True

    # ----------------------------- Monitor -----------------------------

    def conectar_monitor(self) -> socket.socket:
        """Conecta con el Monitor y obtiene el ID. Reintenta hasta conseguirlo."""
        while True:
            sock = None
            try:
                # El timeout vale para todo el socket: si el Monitor deja de mandar
                # HEALTH durante 5 s, recv() lanza socket.timeout y reconectamos.
                sock = socket.create_connection(self.dir_monitor, timeout=TIMEOUT_MONITOR)
                sock.sendall(ENQ)
                if leer_byte(sock) != ACK:
                    raise ConnectionError("el Monitor no respondió ACK al ENQ")

                campos = peticion(sock, "HELLO")            # -> ["ID", "WS-04"]
                if campos[0] != "ID" or len(campos) < 2:
                    raise ConnectionError(f"respuesta inesperada al HELLO: {'#'.join(campos)}")
                if self.ws_id is not None and campos[1] != self.ws_id:
                    # Un Engine pertenece a una sola WS: no puede cambiar de ID a mitad.
                    raise ConnectionError(f"el Monitor es de {campos[1]}, no de {self.ws_id}")

                self.ws_id = campos[1]
                log(f"[OK] Conectado al Monitor. Soy {self.ws_id}")
                return sock
            except OSError as e:
                if sock:
                    sock.close()
                ip, puerto = self.dir_monitor
                log(f"[AVISO] No se puede conectar con el Monitor en {ip}:{puerto} ({e})")
            log(f"Reintento en {ESPERA_RECONEXION} s...")
            time.sleep(ESPERA_RECONEXION)

    def hilo_salud(self, sock: socket.socket) -> None:
        """Contesta a los HEALTH del Monitor. Si lo pierde, reconecta."""
        while True:
            try:
                while True:
                    texto = recibir_trama(sock)
                    if texto != "HEALTH":
                        log(f"[AVISO] Mensaje desconocido del Monitor: {texto}")
                        continue
                    # Se lee el estado con el lock y se suelta ANTES de enviar:
                    # enviar puede tardar (red lenta) y no queremos bloquear el teclado.
                    with self.lock:
                        ok = self.estado_ok
                    enviar_trama(sock, "OK#HEALTH" if ok else "KO#HEALTH")
            except OSError as e:
                log(f"[AVISO] Monitor perdido ({e})")
                sock.close()
            sock = self.conectar_monitor()

    # ------------------------------ Teclado ------------------------------

    def alternar_estado(self) -> None:
        with self.lock:
            self.estado_ok = not self.estado_ok
            ok = self.estado_ok
        if ok:
            log("[OK] Avería reparada: el Monitor recibirá OK#HEALTH")
        else:
            log("[KO] Avería simulada: el Monitor recibirá KO#HEALTH y avisará de FUGA")

    def mostrar_menu(self) -> None:
        with self.lock:
            estado = "OK" if self.estado_ok else "KO (avería)"
        log(f"--- WM_WS_E · {self.ws_id} · Estado: {estado} ---")
        log("    k = simular/reparar avería    q = salir    (y pulsa Enter)")

    def bucle_teclado(self) -> None:
        """input() y no msvcrt: msvcrt solo existe en Windows; input() vale en
        Windows, Linux y Docker."""
        while True:
            self.mostrar_menu()
            opcion = input().strip().lower()
            if opcion == "k":
                self.alternar_estado()
            elif opcion == "q":
                return
            elif opcion:
                log(f"Opción desconocida: {opcion}")


def main() -> None:
    parser = argparse.ArgumentParser(description="WM_WS_E: Engine de una Watering Station")
    parser.add_argument("broker", type=ip_puerto, help="IP y puerto del broker Kafka (ip:puerto)")
    parser.add_argument("monitor", type=ip_puerto, help="IP y puerto del Monitor de la WS (ip:puerto)")
    args = parser.parse_args()

    engine = Engine(args.monitor, args.broker)
    log(f"=== WM_WS_E · broker Kafka {args.broker[0]}:{args.broker[1]} (se usará en R4) ===")
    try:
        # Primero el ID: sin él el Engine no sabe quién es (en R4 lo necesita para Kafka).
        sock = engine.conectar_monitor()
        # daemon=True: el hilo muere cuando termina el principal.
        threading.Thread(target=engine.hilo_salud, args=(sock,), daemon=True).start()
        try:
            engine.bucle_teclado()
        except EOFError:
            # Sin teclado (stdin cerrado, p. ej. Docker sin -it): el menú no sirve,
            # pero el Engine debe seguir contestando al Monitor.
            log("[AVISO] Sin teclado: el Engine sigue funcionando sin menú")
            while True:
                time.sleep(1)
    except KeyboardInterrupt:
        pass
    # Al salir, el sistema operativo cierra el socket y el Monitor lo detecta
    # como 'Engine sin respuesta' -> LEAK. Es lo correcto: sin Engine no se puede regar.
    log("Cerrando el Engine...")


if __name__ == "__main__":
    main()

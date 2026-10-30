"""
WM_WS_M · Monitor de una Watering Station (WS).

Uso (siempre desde la raíz del repositorio, para que funcione 'from common ...'):
    python -m wm_ws.WS_M.WM_WS_M <puerto_engine> <ip_central:puerto> <ws_id> [--ubicacion "..."] [--sin-registro]
    python -m wm_ws.WS_M.WM_WS_M 9100 localhost:9000 WS-04 --ubicacion "River Park"

Qué hace:
  1. Se conecta a CENTRAL y se autentica:  ENQ -> ACK,  AUTH#WS-04#River Park -> OK#AUTH.
     Si CENTRAL contesta KO#AUTH#WS no registrada, se da de alta primero
     (REGISTER#WS-04#River Park -> OK#REGISTER) y repite el AUTH.
     Con --sin-registro no se da de alta: sirve para enseñar la denegación.
  2. Envía ALIVE#WS-04 a CENTRAL cada 2 s, para que CENTRAL sepa que la WS sigue viva
     (CENTRAL la marca DESCONECTADA si pasan 5 s sin ALIVE).
  3. Abre un servidor de sockets y espera a su Engine:  ENQ -> ACK,  HELLO -> ID#WS-04.
  4. Envía HEALTH al Engine cada 1 s. Si contesta KO, tarda más de 3 s o se desconecta,
     avisa a CENTRAL con LEAK. Cuando vuelve a contestar OK, avisa con RECOVERED.

Hilos:
  - Principal: servidor del Engine y latido HEALTH. Solo hay un Engine por WS, así que
    no hace falta un hilo por Engine: se atiende uno y, si se cae, se espera al siguiente.
  - hilo_alive: ALIVE cada 2 s y reconexión con CENTRAL si la conexión se pierde.

Los dos hilos escriben en el MISMO socket de CENTRAL (el principal manda LEAK/RECOVERED,
el otro manda ALIVE). Por eso todo intercambio con CENTRAL va dentro de 'lock_central'.
"""
import argparse
import re
import socket
import sys
import threading
import time

from common.utilidades import crear_servidor, ip_puerto, log
from common.protocol import ACK, ENQ, EOT, enviar_trama, leer_byte, peticion, recibir_trama

INTERVALO_ALIVE = 2      # s entre ALIVE (CENTRAL desconecta la WS a los 5 s sin ALIVE)
INTERVALO_HEALTH = 1     # s entre HEALTH al Engine (lo pide el enunciado)
TIMEOUT_ENGINE = 3       # s sin respuesta del Engine = fuga (contrato)
TIMEOUT_CENTRAL = 5      # s máximos esperando a CENTRAL antes de darla por caída
ESPERA_RECONEXION = 3    # s entre intentos de conexión con CENTRAL

# ---------------------------------------------------------------------------
# Validación de argumentos (argparse llama a estas funciones con el texto tal cual)
# ---------------------------------------------------------------------------

def id_ws(texto: str) -> str:
    """Los IDs llevan guion (contrato): WS-04, WS-11..."""
    if not re.fullmatch(r"WS-\d+", texto):
        raise argparse.ArgumentTypeError(f"'{texto}' no es un ID válido (ejemplo: WS-04)")
    return texto


def ubicacion_valida(texto: str) -> str:
    """'#' separa los campos de la trama: si la ubicación lo llevara, CENTRAL leería
    campos de más. Por eso se prohíbe aquí, antes de enviarla."""
    if not texto.strip() or "#" in texto:
        raise argparse.ArgumentTypeError("la ubicación no puede estar vacía ni contener '#'")
    return texto.strip()


# ---------------------------------------------------------------------------
# Monitor
# ---------------------------------------------------------------------------

class Monitor:
    def __init__(self, ws_id: str, ubicacion: str, dir_central: tuple[str, int],
                 registrar: bool = True):
        self.ws_id = ws_id
        self.ubicacion = ubicacion
        self.dir_central = dir_central
        self.registrar = registrar        # False con --sin-registro

        self.lock_central = threading.Lock()
        self.sock_central = None          # None = sin conexión con CENTRAL

        # Solo los modifica el hilo principal (el del Engine). El hilo ALIVE solo
        # los lee al reconectar; leer un bool o un str es atómico en Python.
        self.fuga = False
        self.motivo_fuga = ""

    # ---------------------------- CENTRAL ----------------------------

    def conectar_central(self, primera_vez: bool) -> None:
        """Conecta con CENTRAL y se autentica. Reintenta hasta conseguirlo.

        Solo termina el programa si CENTRAL rechaza el AUTH en el primer arranque
        (por ejemplo, 'ID ya conectado'): no tiene sentido seguir con un ID inválido.
        Si el rechazo llega al reconectar, se sigue intentando: puede que CENTRAL
        aún no haya notado que la conexión anterior se cayó.
        """
        while True:
            sock = None
            try:
                # timeout: si CENTRAL se cuelga, recv() lanza socket.timeout en vez de
                # bloquear el hilo (y el lock) para siempre.
                sock = socket.create_connection(self.dir_central, timeout=TIMEOUT_CENTRAL)
                sock.sendall(ENQ)
                if leer_byte(sock) != ACK:
                    raise ConnectionError("CENTRAL no respondió ACK al ENQ")

                campos = peticion(sock, f"AUTH#{self.ws_id}#{self.ubicacion}")
                if self._no_registrada(campos) and self.registrar:
                    # Primera vez de esta WS: alta en CENTRAL y otra vez AUTH.
                    log(f"[AVISO] CENTRAL: {self.ws_id} no está registrada. Dándola de alta...")
                    alta = peticion(sock, f"REGISTER#{self.ws_id}#{self.ubicacion}")
                    if alta[0] == "OK":
                        log(f"[OK] {self.ws_id} registrada en CENTRAL ({self.ubicacion})")
                    else:
                        log(f"[ERROR] CENTRAL rechaza el registro: "
                            f"{alta[2] if len(alta) > 2 else 'sin motivo'}")
                    campos = peticion(sock, f"AUTH#{self.ws_id}#{self.ubicacion}")
                if campos[0] == "OK":
                    break                                     # autenticado

                sock.close()
                motivo = campos[2] if len(campos) > 2 else "sin motivo"
                log(f"[ERROR] CENTRAL rechaza la autenticación: {motivo}")
                if primera_vez:
                    sys.exit(1)
            except OSError as e:
                # OSError incluye ConnectionError, socket.timeout e IOError.
                if sock:
                    sock.close()
                ip, puerto = self.dir_central
                log(f"[AVISO] No se puede conectar con CENTRAL en {ip}:{puerto} ({e})")
            log(f"Reintento en {ESPERA_RECONEXION} s...")
            time.sleep(ESPERA_RECONEXION)

        # Se publica el socket y se sincroniza la salud de una vez, con el lock cogido,
        # para que el hilo del Engine no pueda colar un LEAK en medio.
        with self.lock_central:
            self.sock_central = sock
            log(f"[OK] Autenticado en CENTRAL como {self.ws_id} ({self.ubicacion})")
            # Al autenticarse, CENTRAL da la WS por sana. Si se reconecta en mitad de
            # una fuga, hay que recordárselo o la vería en verde.
            if self.fuga:
                self._peticion_central(f"LEAK#{self.ws_id}#{self.motivo_fuga}")

    @staticmethod
    def _no_registrada(campos: list) -> bool:
        return campos[:2] == ["KO", "AUTH"] and len(campos) > 2 and "no registrada" in campos[2]

    def _peticion_central(self, texto: str) -> bool:
        """Envía 'texto' a CENTRAL y comprueba que responde OK.

        Hay que llamarla con lock_central cogido. El lock cubre la petición ENTERA
        (enviar + esperar respuesta): si no, un hilo podría leerse la respuesta del
        otro (tomar el OK#ALIVE como respuesta a su LEAK).
        """
        if self.sock_central is None:
            return False                      # CENTRAL caída: el hilo ALIVE reconectará
        try:
            return peticion(self.sock_central, texto)[0] == "OK"
        except OSError as e:
            log(f"[AVISO] Conexión con CENTRAL perdida ({e})")
            self.sock_central.close()
            self.sock_central = None
            return False

    def enviar_central(self, texto: str) -> bool:
        with self.lock_central:
            return self._peticion_central(texto)

    def hilo_alive(self) -> None:
        """Cada 2 s: ALIVE si hay conexión; si no, reconecta."""
        while True:
            time.sleep(INTERVALO_ALIVE)
            if self.sock_central is None:
                log("Reconectando con CENTRAL...")
                self.conectar_central(primera_vez=False)
            else:
                self.enviar_central(f"ALIVE#{self.ws_id}")

    # ------------------------------ Salud ------------------------------

    def marcar_fuga(self, motivo: str) -> None:
        """Avisa a CENTRAL solo en el cambio OK -> FUGA, no cada segundo.
        Si CENTRAL está caída, el aviso se pierde, pero 'fuga' queda en True y
        conectar_central() lo reenvía al reconectar."""
        if self.fuga:
            return
        self.fuga = True
        self.motivo_fuga = motivo
        log(f"[FUGA] {motivo} -> aviso a CENTRAL (LEAK)")
        self.enviar_central(f"LEAK#{self.ws_id}#{motivo}")

    def marcar_ok(self) -> None:
        """Avisa a CENTRAL solo en el cambio FUGA -> OK."""
        if not self.fuga:
            return
        self.fuga = False
        log("[OK] El Engine vuelve a responder bien -> aviso a CENTRAL (RECOVERED)")
        self.enviar_central(f"RECOVERED#{self.ws_id}")

    # ------------------------------ Engine ------------------------------

    def servidor_engine(self, puerto: int) -> None:
        srv = crear_servidor(puerto)
        srv.listen(1)
        # En Windows, un accept() bloqueado no deja que Ctrl+C pare el programa.
        # Con timeout, accept() vuelve cada segundo y Python puede atender el Ctrl+C.
        srv.settimeout(1)

        log(f"Esperando al Engine en el puerto {puerto}...")
        try:
            while True:
                try:
                    conn, addr = srv.accept()
                except socket.timeout:   # en Python 3.9 NO es TimeoutError; desde 3.10 son lo mismo
                    continue
                self.atender_engine(conn, addr)
                log(f"Esperando de nuevo al Engine en el puerto {puerto}...")
        finally:
            srv.close()

    def atender_engine(self, conn: socket.socket, addr) -> None:
        """Presentación con el Engine y latido HEALTH hasta que se caiga."""
        presentado = False
        try:
            # 3 s para cualquier recv(): un Engine mudo provoca socket.timeout = fuga.
            conn.settimeout(TIMEOUT_ENGINE)

            # El ENQ hay que leerlo a mano: recibir_trama() lo descartaría como basura.
            if leer_byte(conn) != ENQ:
                raise ConnectionError("no empezó con ENQ")
            conn.sendall(ACK)
            if recibir_trama(conn) != "HELLO":
                raise ConnectionError("no se presentó con HELLO")
            enviar_trama(conn, f"ID#{self.ws_id}")    # el Engine recibe su ID de nosotros
            presentado = True
            log(f"[OK] Engine conectado desde {addr[0]}:{addr[1]}")

            while True:
                enviar_trama(conn, "HEALTH")
                if recibir_trama(conn) == "OK#HEALTH":
                    self.marcar_ok()
                else:                                  # KO#HEALTH o cualquier cosa rara
                    self.marcar_fuga("Engine KO")
                time.sleep(INTERVALO_HEALTH)

        except OSError as e:
            if presentado:
                log(f"[AVISO] Engine perdido ({e})")
                self.marcar_fuga("Engine sin respuesta")
            else:
                # Alguien conectó al puerto pero no habla nuestro protocolo: se ignora.
                log(f"[AVISO] Conexión de {addr[0]} descartada ({e})")
        finally:
            conn.close()

    # ------------------------------ Cierre ------------------------------

    def cerrar(self) -> None:
        """EOT = cierre ordenado: CENTRAL sabe que nos vamos a propósito."""
        with self.lock_central:
            if self.sock_central:
                try:
                    self.sock_central.sendall(EOT)
                except OSError:
                    pass
                self.sock_central.close()
                self.sock_central = None


def main() -> None:
    parser = argparse.ArgumentParser(description="WM_WS_M: Monitor de una Watering Station")
    parser.add_argument("puerto_engine", type=int, help="puerto donde se conectará el Engine")
    parser.add_argument("central", type=ip_puerto, help="IP y puerto de WM_Central (ip:puerto)")
    parser.add_argument("ws_id", type=id_ws, help="ID de la estación, por ejemplo WS-04")
    parser.add_argument("--ubicacion", type=ubicacion_valida, default="Sin ubicación",
                        help='parque o zona, por ejemplo "River Park"')
    parser.add_argument("--sin-registro", action="store_true",
                        help="no darse de alta si CENTRAL no la conoce (para probar la denegación)")
    args = parser.parse_args()

    monitor = Monitor(args.ws_id, args.ubicacion, args.central,
                      registrar=not args.sin_registro)
    log(f"=== WM_WS_M · {args.ws_id} · {args.ubicacion} ===")
    try:
        monitor.conectar_central(primera_vez=True)
        # daemon=True: el hilo muere solo cuando termina el principal (al pulsar Ctrl+C).
        threading.Thread(target=monitor.hilo_alive, daemon=True).start()
        monitor.servidor_engine(args.puerto_engine)
    except KeyboardInterrupt:
        log("Cerrando el Monitor...")
    except OSError as e:
        log(f"[ERROR] {e}")                   # por ejemplo, el puerto del Engine ya está en uso
    finally:
        monitor.cerrar()


if __name__ == "__main__":
    main()

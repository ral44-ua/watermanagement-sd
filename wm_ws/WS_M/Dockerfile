# Imagen de WM_WS_M (Monitor de una Watering Station).
#
# Se construye desde la RAÍZ del repo, porque necesita también common/:
#     docker build -f wm_ws/WS_M/Dockerfile -t wm-ws-m .
#
# Ejecutar (los argumentos son los mismos que con python -m):
#     docker network create wm                      (una vez por PC)
#     docker run -it --rm --name ws04-m --network wm -p 9100:9100 \
#         wm-ws-m 9100 <ip_central:puerto> WS-04 --ubicacion "River Park"
#
#   --name ws04-m   el Engine de esta WS se conecta a "ws04-m:9100"
#   --network wm    red de Docker compartida con el Engine
#   -p 9100:9100    solo si el Engine va a correr FUERA de Docker
#   -it             para ver los mensajes en la terminal y parar con Ctrl+C

FROM python:3.12-slim

# Los print salen al momento; sin carpetas __pycache__.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# El Monitor solo usa sockets (no Kafka), pero se instala requirements.txt
# igual que en los demás módulos: es pequeño y así todas las imágenes son iguales.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY wm_ws/__init__.py wm_ws/__init__.py
COPY wm_ws/WS_M/ wm_ws/WS_M/

# Puerto donde espera a su Engine (informativo: el real es el primer argumento).
EXPOSE 9100

# ENTRYPOINT + argumentos de "docker run": todo se parametriza al arrancar,
# sin reconstruir la imagen.
ENTRYPOINT ["python", "-m", "wm_ws.WS_M.WM_WS_M"]

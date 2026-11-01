# Imagen de WM_Central (CENTRAL + su BD SQLite + panel web).
#
# Se construye desde la RAÍZ del repo, porque necesita también common/:
#     docker build -f WM_Central/Dockerfile -t wm-central .
# Probar en local (con Kafka accesible en la dirección de BROKER):
#     docker run --rm -p 9000:9000 -p 8080:8080 -e BROKER=<ip:puerto> wm-central
#
# En Railway: variable RAILWAY_DOCKERFILE_PATH=WM_Central/Dockerfile y un
# volumen montado en /data (ahí vive la BD). Ver docs/railway.md.

# Imagen base pequeña con Python 3.12 (slim = sin compiladores ni extras).
FROM python:3.12-slim

# PYTHONUNBUFFERED: los print salen al momento en los logs (sin esperar a
# llenar un búfer). PYTHONDONTWRITEBYTECODE: no crea carpetas __pycache__.
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

# Primero solo requirements.txt y la instalación: Docker guarda cada paso en
# caché, y así, si solo cambia el código, no vuelve a instalar las librerías.
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY WM_Central/ WM_Central/

# Valores por defecto. En Railway (o con "docker run -e") se cambian.
#   PUERTO_SOCKETS  servidor de sockets para los WM_WS_M
#   BROKER          ip:puerto del bróker Kafka
#   BD              fichero SQLite (en /data para que sobreviva a redespliegues)
#   PORT            panel web (Railway llama así a este puerto)
ENV PUERTO_SOCKETS=9000 \
    BROKER=localhost:9092 \
    BD=/data/wm_central.db \
    PORT=8080

# Railway no admite la instrucción VOLUME: el volumen se crea en su web y se
# monta en /data. Aquí solo se crea la carpeta, por si se arranca sin volumen.
RUN mkdir -p /data

# Solo informativo: qué puertos usa el contenedor.
EXPOSE 9000 8080

# Forma "shell" (sh -c) para que se sustituyan las variables $...
# "exec" hace que python sustituya a sh y sea el proceso principal: así es
# python quien recibe el SIGTERM de "docker stop" o de Railway y cierra Kafka
# de forma ordenada (ver WM_Central.main). Sin exec, la señal le llegaría a sh.
# --sin-panel: en un contenedor no hay pantalla; los eventos salen por los logs.
CMD exec python -m WM_Central.WM_Central "$PUERTO_SOCKETS" "$BROKER" --bd "$BD" --sin-panel

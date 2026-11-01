# Imagen de WM_WS_E (Engine de una Watering Station).
#
# Se construye desde la RAÍZ del repo, porque necesita también common/:
#     docker build -f WM_WS/WS_E/Dockerfile -t wm-ws-e .
#
# Ejecutar (después de su Monitor, en la misma red de Docker):
#     docker run -it --rm --name ws04-e --network wm -v wm-ws04:/datos \
#         wm-ws-e <ip_kafka:puerto> ws04-m:9100
#
#   -it               el Engine tiene menú de teclado (k, p, r, q)
#   ws04-m:9100       nombre del contenedor del Monitor y su puerto
#   -v wm-ws04:/datos volumen donde se guarda el riego en curso. Si el Engine
#                     se cae regando, al volver a lanzarlo con el MISMO volumen
#                     envía a CENTRAL el estado final de ese riego.

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    WM_ESTADO_DIR=/datos

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY WM_WS/__init__.py WM_WS/__init__.py
COPY WM_WS/WS_E/ WM_WS/WS_E/

# Carpeta del fichero riego_<WS>.json (WM_ESTADO_DIR). Se crea por si se
# arranca sin volumen; con -v, Docker monta el volumen encima.
RUN mkdir -p /datos

ENTRYPOINT ["python", "-m", "WM_WS.WS_E.WM_WS_E"]

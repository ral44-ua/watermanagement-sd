# Imagen de WM_FO (aplicación del operario).
#
# Se construye desde la RAÍZ del repo, porque necesita también common/:
#     docker build -f WM_FO/Dockerfile -t wm-fo .
#
# Ejecutar:
#     docker run -it --rm wm-fo <ip_kafka:puerto> FO-12
#     docker run -it --rm wm-fo <ip_kafka:puerto> FO-07 --fichero WM_FO/peticiones.txt
#
# Con otro fichero de peticiones del PC, se monta en el contenedor:
#     docker run -it --rm -v "$(pwd)/mis.txt:/app/mis.txt" wm-fo <ip_kafka:puerto> FO-07 --fichero mis.txt
#
#   -it   el FO tiene menú de teclado
#   Usad siempre el mismo ID al relanzar un FO: así recibe los avisos que se
#   perdió mientras estaba cerrado (Kafka los guarda para su grupo wm-fo-<ID>).

FROM python:3.12-slim

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

COPY common/ common/
COPY WM_FO/ WM_FO/

ENTRYPOINT ["python", "-m", "WM_FO.WM_FO"]

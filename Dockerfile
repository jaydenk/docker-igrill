FROM python:3.12-alpine

ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

WORKDIR /app

COPY requirements.txt /app/requirements.txt

RUN apk add --no-cache dbus bluez libffi \
    && apk add --no-cache --virtual .build-deps build-base libffi-dev \
    && pip install --no-cache-dir -r /app/requirements.txt \
    && apk del .build-deps \
    && mkdir -p /data

COPY service /app/service
COPY README.md /app/README.md

EXPOSE 39120

CMD ["python", "-m", "service.main"]

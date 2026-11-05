FROM python:3.12-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt && useradd -m room
COPY . .
RUN mkdir -p /app/data && chown -R room:room /app/data
USER room
ENV ROOM_SIX_DB=/app/data/room-six.db
CMD ["sh","-c","exec uvicorn app:app --host 0.0.0.0 --port ${PORT:-8000} --workers 1"]

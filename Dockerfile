FROM python:3.11-slim

WORKDIR /app

# Устанавливаем системные зависимости
RUN apt-get update && apt-get install -y \
    gcc \
    python3-dev \
    && rm -rf /var/lib/apt/lists/*

# Копируем зависимости и устанавливаем их
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Создаем папку proto и добавляем __init__.py
RUN mkdir -p proto && touch proto/__init__.py

# Копируем исходные файлы
COPY . .

# Компилируем protobuf
RUN python -m grpc_tools.protoc \
    -I=. \
    --python_out=. \
    proto/matcher.proto


CMD ["python", "server.py"]
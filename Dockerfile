FROM python:3.11-slim

# システム依存パッケージ（Playwright + CJKフォント）
RUN apt-get update && apt-get install -y --no-install-recommends \
    wget \
    curl \
    gnupg \
    fonts-noto-cjk \
    libnss3 \
    libatk1.0-0 \
    libatk-bridge2.0-0 \
    libcups2 \
    libdrm2 \
    libxkbcommon0 \
    libxcomposite1 \
    libxdamage1 \
    libxfixes3 \
    libxrandr2 \
    libgbm1 \
    libasound2 \
    && rm -rf /var/lib/apt/lists/*

WORKDIR /app

# requirements を先にコピー（レイヤーキャッシュ活用）
COPY requirements.txt .

# Python パッケージインストール
RUN pip install --no-cache-dir -r requirements.txt

# Playwright ブラウザインストール
RUN playwright install chromium --with-deps

# アプリケーションコードをコピー
COPY . .

ENV PYTHONUNBUFFERED=1
ENV PORT=8000
EXPOSE $PORT

CMD ["python3", "run_server.py"]

#!/bin/bash
# Shopee Profit Finder — Mac 起動スクリプト
# ターミナルで: bash start.sh

set -e
cd "$(dirname "$0")"

echo "========================================="
echo "  Shopee Profit Finder"
echo "========================================="
echo ""

# Python3 確認
if ! command -v python3 &>/dev/null; then
  echo "ERROR: python3 が見つかりません。"
  echo "  → https://www.python.org からインストールしてください"
  exit 1
fi
echo "✓ Python: $(python3 --version)"

# 必要な最低限パッケージのみ確認・インストール
echo ""
echo "依存パッケージを確認中..."

install_if_missing() {
  python3 -c "import $1" 2>/dev/null && echo "✓ $1" || {
    echo "  → $2 をインストール中..."
    pip3 install "$2" --quiet
    echo "✓ $2 インストール完了"
  }
}

install_if_missing tornado tornado
install_if_missing jinja2 jinja2

echo ""
echo "----------------------------------------"
echo "  サーバーを起動します"
echo "  URL: http://localhost:8000"
echo "  停止: Ctrl+C"
echo "----------------------------------------"
echo ""

open "http://localhost:8000" 2>/dev/null &   # Mac の場合ブラウザを自動で開く
sleep 1
python3 run_server.py

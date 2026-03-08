#!/usr/bin/env python3
# start.py — запускает бота и API одновременно

import threading
import subprocess
import sys
import os

def run_bot():
    subprocess.run([sys.executable, "bot_enhanced.py"])

def run_api():
    subprocess.run([
        sys.executable, "-m", "uvicorn",
        "api:app",
        "--host", "0.0.0.0",
        "--port", "8000"
    ])

if __name__ == "__main__":
    print("🚀 Запуск бота и API...")

    bot_thread = threading.Thread(target=run_bot, daemon=True)
    api_thread = threading.Thread(target=run_api, daemon=True)

    bot_thread.start()
    api_thread.start()

    bot_thread.join()
    api_thread.join()

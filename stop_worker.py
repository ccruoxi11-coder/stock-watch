"""请求本机后台采集器安全退出。"""
from __future__ import annotations

import socket

from worker import LOCK_PORT


def main() -> int:
    try:
        with socket.create_connection(("127.0.0.1", LOCK_PORT), timeout=3) as connection:
            connection.sendall(b"STOP")
            response = connection.recv(32)
        print("后台采集器已收到停止指令。" if response == b"OK" else "停止响应异常。")
        return 0
    except OSError:
        print("后台采集器未运行。")
        return 1


if __name__ == "__main__":
    raise SystemExit(main())

import os
import json
import threading
import socketserver
from pathlib import Path

MAX_LINE = 64 * 1024  # límite anti-bloat

# ==== Persistencia (JSON) ====
DATA_DIR = Path(os.getenv("DATA_DIR", "/data"))
DATA_DIR.mkdir(parents=True, exist_ok=True)
DB_FILE = DATA_DIR / "db.json"

def load_store():
    if DB_FILE.exists():
        try:
            with DB_FILE.open("r", encoding="utf-8") as f:
                return json.load(f)
        except Exception:
            return {}
    return {}

def save_store(store: dict):
    tmp = DB_FILE.with_suffix(".json.tmp")
    with tmp.open("w", encoding="utf-8") as f:
        json.dump(store, f, ensure_ascii=False)
        f.flush()
        os.fsync(f.fileno())
    os.replace(tmp, DB_FILE)

# ==== Handler ====
class KVHandler(socketserver.StreamRequestHandler):
    def sendline(self, s: str):
        if not s.endswith("\n"):
            s += "\n"
        self.wfile.write(s.encode("utf-8"))
        self.wfile.flush()
        print(f"[srv] -> {s.strip()}", flush=True)

    def handle(self):
        self.sendline("OK KV-STORE READY")

        buf = b""
        while True:
            chunk = self.request.recv(4096)
            if not chunk:
                break
            buf += chunk

            # aceptar \n o \r (CRLF/CR/LF)
            while True:
                idx_n = buf.find(b"\n")
                idx_r = buf.find(b"\r")
                candidates = [i for i in (idx_n, idx_r) if i != -1]
                if not candidates:
                    break
                i = min(candidates)
                line = buf[:i]
                j = i + 1
                if len(buf) > i + 1 and (
                    (buf[i] == 13 and buf[i+1] == 10) or
                    (buf[i] == 10 and buf[i+1] == 13)
                ):
                    j = i + 2
                buf = buf[j:]

                text = line.decode("utf-8", "replace").strip()
                if not text:
                    continue

                parts = text.split(" ", 2)  # SET k v (v con espacios)
                cmd = parts[0].upper()

                try:
                    if cmd == "SET" and len(parts) >= 3:
                        key, value = parts[1], parts[2]
                        with self.server.lock:
                            self.server.store[key] = value
                            save_store(self.server.store)
                        self.sendline("OK")

                    elif cmd == "GET" and len(parts) == 2:
                        key = parts[1]
                        with self.server.lock:
                            val = self.server.store.get(key)
                        self.sendline("NIL" if val is None else f"VALUE {val}")

                    elif cmd == "DEL" and len(parts) == 2:
                        key = parts[1]
                        with self.server.lock:
                            existed = key in self.server.store
                            if existed:
                                del self.server.store[key]
                                save_store(self.server.store)
                        self.sendline("OK" if existed else "NIL")

                    elif cmd == "KEYS":
                        with self.server.lock:
                            keys = " ".join(self.server.store.keys())
                        self.sendline(f"VALUE {keys}")

                    elif cmd in ("QUIT", "EXIT"):
                        self.sendline("BYE")
                        return

                    else:
                        self.sendline("ERR unknown command or bad args")
                except Exception as e:
                    self.sendline(f"ERR server error: {type(e).__name__}")

# ==== Server ====
class KVServer(socketserver.ThreadingTCPServer):
    allow_reuse_address = True
    daemon_threads = True

def main():
    host = os.getenv("HOST", "0.0.0.0")
    port = int(os.getenv("PORT", "5000"))
    with KVServer((host, port), KVHandler) as server:
        server.store = load_store()
        server.lock = threading.RLock()
        print(f"KV server listening on {host}:{port}", flush=True)
        try:
            server.serve_forever()
        except KeyboardInterrupt:
            print("Shutting down...", flush=True)

if __name__ == "__main__":
    main()

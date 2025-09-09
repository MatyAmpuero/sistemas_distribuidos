import os
import socket
import sys

SERVER_HOST = os.getenv("SERVER_HOST", "127.0.0.1")
SERVER_PORT = int(os.getenv("SERVER_PORT", "5000"))
RECV_BUFSIZE = 64 * 1024

def connect():
    s = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    s.connect((SERVER_HOST, SERVER_PORT))
    return s

def main():
    print(f"KV-CLIENT connecting to {SERVER_HOST}:{SERVER_PORT} ...")
    try:
        s = connect()
    except Exception as e:
        print(f"ERROR: cannot connect: {e}")
        sys.exit(1)

    # banner (no bloqueante)
    try:
        s.settimeout(1.0)
        try:
            banner = s.recv(RECV_BUFSIZE)
            if banner:
                print(banner.decode("utf-8", "replace").rstrip())
        except socket.timeout:
            pass
        finally:
            s.settimeout(None)
    except Exception:
        pass

    print("Escribe comandos: SET <k> <v> | GET <k> | DEL <k> | KEYS | QUIT")
    print("Ctrl+D/Ctrl+Z para salir.\n")

    try:
        with s:
            while True:
                try:
                    line = input("> ").strip()
                except EOFError:
                    print("\nbye")
                    break
                if not line:
                    continue

                try:
                    s.sendall((line + "\n").encode("utf-8"))
                except BrokenPipeError:
                    print("Conexión cerrada por el servidor.")
                    break

                data = b""
                s.settimeout(3.0)  # evita cuelgue infinito
                try:
                    while True:
                        chunk = s.recv(4096)
                        if not chunk:
                            print("Conexión cerrada por el servidor.")
                            return
                        data += chunk
                        if b"\n" in data:
                            break
                except socket.timeout:
                    print("[timeout] sin respuesta del servidor")
                    continue
                finally:
                    s.settimeout(None)

                print(data.decode("utf-8", "replace").rstrip())

                if line.upper() in ("QUIT", "EXIT"):
                    break
    except KeyboardInterrupt:
        print("\nbye")

if __name__ == "__main__":
    main()

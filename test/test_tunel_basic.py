import paramiko
import socket
import select
import threading
import oracledb

# Configuració SSH
SSH_HOST = "dcccluster.uab.cat"
SSH_PORT = 8192
SSH_USER = "student"
SSH_KEY = "../dev_keys/id_student"   # o None si vols password
SSH_PASSWORD = None                  # si no fas servir clau privada

# Configuració Oracle
ORACLE_HOST = "oracle-1.grup00.gabd"   # es veurà des del servidor SSH
ORACLE_PORT = 1521
ORACLE_SID = "FREE"
ORACLE_SERVICE_NAME = "FREEPDB1"
ORACLE_USER = "ESPECTACLES"
ORACLE_PASS = "ESPECTACLES"

# Port local per al túnel
LOCAL_PORT = 1521


def forward_tunnel(local_port, remote_host, remote_port, transport):
    """Crea el túnel local cap a Oracle via SSH."""
    class Handler (threading.Thread):
        def __init__(self, chan, sock):
            threading.Thread.__init__(self)
            self.chan = chan
            self.sock = sock

        def run(self):
            while True:
                r, w, x = select.select([self.sock, self.chan], [], [])
                if self.sock in r:
                    data = self.sock.recv(1024)
                    if len(data) == 0:
                        break
                    self.chan.send(data)
                if self.chan in r:
                    data = self.chan.recv(1024)
                    if len(data) == 0:
                        break
                    self.sock.send(data)

            self.chan.close()
            self.sock.close()

    def accept(sock):
        while True:
            client, addr = sock.accept()
            chan = transport.open_channel("direct-tcpip",
                                          (remote_host, remote_port),
                                          client.getpeername())
            thr = Handler(chan, client)
            thr.setDaemon(True)
            thr.start()

    sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
    sock.bind(("localhost", local_port))
    sock.listen(100)
    threading.Thread(target=accept, args=(sock,), daemon=True).start()


def main():
    # Connecta via SSH
    client = paramiko.SSHClient()
    client.set_missing_host_key_policy(paramiko.AutoAddPolicy())

    if SSH_KEY:
        client.connect(SSH_HOST, SSH_PORT, username=SSH_USER, key_filename=SSH_KEY)
    else:
        client.connect(SSH_HOST, SSH_PORT, username=SSH_USER, password=SSH_PASSWORD)

    transport = client.get_transport()
    forward_tunnel(LOCAL_PORT, ORACLE_HOST, ORACLE_PORT, transport)

    print(f"Túnel creat: localhost:{LOCAL_PORT} → {ORACLE_HOST}:{ORACLE_PORT}")

    # Connexió Oracle a través del túnel
    dsn = oracledb.makedsn("localhost", LOCAL_PORT, service_name=ORACLE_SERVICE_NAME)
    conn = oracledb.connect(user=ORACLE_USER, password=ORACLE_PASS, dsn=dsn)

    cursor = conn.cursor()
    cursor.execute("SELECT 'Hello from Oracle!' FROM dual")
    for row in cursor:
        print(row)

    cursor.close()
    conn.close()
    client.close()


if __name__ == "__main__":
    main()

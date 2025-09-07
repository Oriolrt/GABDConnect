import paramiko
import socket
import threading
import select

class sshTunnel:
    def __init__(self, ssh_address_or_host, ssh_port=22,
                 ssh_username=None, ssh_password=None, ssh_pkey=None,
                 remote_bind_addresses=None, local_bind_addresses=None):
        if isinstance(ssh_address_or_host, tuple):
            self.ssh_host, self.ssh_port = ssh_address_or_host
        else:
            self.ssh_host, self.ssh_port = ssh_address_or_host, ssh_port

        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_pkey = ssh_pkey

        # Normalitzar a llistes
        self.remote_bind_addresses = remote_bind_addresses or []
        self.local_bind_addresses = local_bind_addresses or []

        # Si no hi ha local binds, assigna automàticament
        if not self.local_bind_addresses:
            self.local_bind_addresses = [("127.0.0.1", 0)] * len(self.remote_bind_addresses)

        self.client = None
        self.transport = None
        self._threads = []
        self._sockets = []

    @property
    def local_bind_ports(self):
        return [sock.getsockname()[1] for sock in self._sockets]

    @property
    def local_bind_port(self):
        # Compatibilitat amb codi que espera un únic port
        ports = self.local_bind_ports
        return ports[0] if ports else None

    def start(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(
            self.ssh_host,
            port=self.ssh_port,
            username=self.ssh_username,
            password=self.ssh_password,
            key_filename=self.ssh_pkey
        )
        self.transport = self.client.get_transport()

        # Crear un socket per a cada bind
        for local_bind, remote_bind in zip(self.local_bind_addresses, self.remote_bind_addresses):
            local_host, local_port = local_bind
            remote_host, remote_port = remote_bind

            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            sock.bind((local_host, local_port))
            local_port = sock.getsockname()[1]
            sock.listen(100)
            self._sockets.append(sock)

            thread = threading.Thread(target=self._forward, args=(sock, remote_host, remote_port), daemon=True)
            thread.start()
            self._threads.append(thread)

    def stop(self):
        for sock in self._sockets:
            sock.close()
        if self.transport:
            self.transport.close()
        if self.client:
            self.client.close()

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def _forward(self, sock, remote_host, remote_port):
        while True:
            try:
                client_sock, _ = sock.accept()
            except OSError:
                break

            chan = self.transport.open_channel(
                "direct-tcpip",
                (remote_host, remote_port),
                client_sock.getsockname(),
            )

            threading.Thread(target=self._handler, args=(client_sock, chan), daemon=True).start()

    def _handler(self, client_sock, chan):
        while True:
            r, _, _ = select.select([client_sock, chan], [], [])
            if client_sock in r:
                data = client_sock.recv(1024)
                if not data:
                    break

    def is_active(self, timeout=2):
        """
        Comprova si el túnel SSH està actiu provant de connectar-se
        a cada port local assignat.
        Retorna True si almenys un túnel està viu.
        """

        if not self.transport or not self.transport.is_active():
            return False

        for sock in self._sockets:
            host, port = sock.getsockname()
            try:
                test = socket.create_connection((host, port), timeout=timeout)
                test.close()
                return True
            except Exception:
                continue
        return False


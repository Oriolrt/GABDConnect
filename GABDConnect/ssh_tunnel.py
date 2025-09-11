# -*- coding: utf-8 -*-
u"""
Created on Jun 12, 2018

"""

import paramiko
import threading
import socket
import select
import time
import gc





class sshTunnel:
    def __init__(self, ssh_address_or_host, ssh_port=22, ssh_username=None, ssh_password=None, ssh_pkey=None,
                 remote_bind_addresses=None, local_bind_addresses=None):
        self.ssh_host, self.ssh_port = ssh_address_or_host if isinstance(ssh_address_or_host, tuple) else (ssh_address_or_host, ssh_port)
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_pkey = ssh_pkey
        self.remote_bind_addresses = remote_bind_addresses or []
        self.local_bind_addresses = local_bind_addresses or [("localhost", 0)] * len(self.remote_bind_addresses)
        self.client = self.transport = None
        self._sockets = []

    @property
    def local_bind_ports(self):
        return [sock.getsockname()[1] for sock in self._sockets]

    @property
    def local_bind_port(self):
        return self.local_bind_ports[0] if self.local_bind_ports else None

    def forward_tunnel(self, local_port, remote_host, remote_port):
        """Create a local tunnel to Oracle via SSH."""

        self._running = True  # Flag per controlar el bucle accept

        def is_port_available(port, host="localhost"):
            with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
                s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
                try:
                    s.bind((host, port))
                    return True
                except OSError:
                    return False

        if not is_port_available(local_port):
            raise RuntimeError(f"El port {local_port} ja està en ús. Tria un altre port o espera uns segons.")

        class Handler(threading.Thread):
            def __init__(self, chan, sock):
                super().__init__()
                self.chan = chan
                self.sock = sock

            def run(self):
                try:
                    while True:
                        r, _, _ = select.select([self.sock, self.chan], [], [])
                        if self.sock in r:
                            try:
                                data = self.sock.recv(1024)
                                if not data:
                                    break
                                if not self.chan.closed:
                                    self.chan.send(data)
                            except (OSError, socket.error):
                                break

                        if self.chan in r:
                            try:
                                data = self.chan.recv(1024)
                                if not data:
                                    break
                                if self.sock:
                                    self.sock.send(data)
                            except (OSError, socket.error):
                                break
                finally:
                    try:
                        self.chan.close()
                    except Exception:
                        pass
                    try:
                        self.sock.close()
                    except Exception:
                        pass

        def accept(sock):
            while self._running:
                try:
                    client, _ = sock.accept()
                    chan = self.transport.open_channel("direct-tcpip", (remote_host, remote_port), client.getpeername())
                    Handler(chan, client).start()
                except OSError:
                    break

        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind(("localhost", local_port))
        sock.listen(100)
        threading.Thread(target=accept, args=(sock,), daemon=True).start()
        self._sockets.append(sock)

    def start(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.ssh_host, port=self.ssh_port, username=self.ssh_username,
                            password=self.ssh_password, key_filename=self.ssh_pkey)
        self.transport = self.client.get_transport()

        for local_bind, remote_bind in zip(self.local_bind_addresses, self.remote_bind_addresses):
            self.forward_tunnel(local_bind[1], remote_bind[0], remote_bind[1])

    def stop(self):
        self._running = False  # Atura el bucle accept
        for sock in self._sockets:
            sock.close()
        self._sockets.clear()

        if self.transport:
            self.transport.close()
            self.transport = None

        if self.client:
            self.client.close()
            self.client = None

        gc.collect()
        time.sleep(2)

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def is_active(self, timeout=2):
        if not self.transport or not self.transport.is_active():
            return False

        for sock in self._sockets:
            try:
                test = socket.create_connection(sock.getsockname(), timeout=timeout)
                test.close()
                return True
            except Exception:
                continue
        return False
# -*- coding: utf-8 -*-
u"""
Created on Jun 12, 2018

"""

import gc
import select
import socket
import threading
import time

import paramiko


def _is_port_free(port: int) -> bool:
    """Comprova si un port està lliure intentant fer-hi bind."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        try:
            s.bind(("", port))
            return True
        except OSError:
            return False


class sshTunnel:
    _local_ports_in_use = set()

    def __init__(self, ssh_address_or_host, ssh_port=22, ssh_username=None, ssh_password=None, ssh_pkey=None,
                 remote_bind_addresses=None, local_bind_addresses=None):
        self.ssh_host, self.ssh_port = ssh_address_or_host if isinstance(ssh_address_or_host, tuple) else (
            ssh_address_or_host, ssh_port)
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_pkey = ssh_pkey
        self.remote_bind_addresses = remote_bind_addresses or []
        self.local_bind_addresses = local_bind_addresses or [("localhost", 0)] * len(self.remote_bind_addresses)
        self.client = self.transport = None
        self._sockets = []
        self._monitor_thread = None
        self._running = {}  # {local_port: True/False}
        self._forward_threads = {}

    @property
    def local_bind_ports(self):
        return [sock.getsockname()[1] for sock in self._sockets]

    @property
    def local_bind_port(self):
        return self.local_bind_ports[0] if self.local_bind_ports else None

    def forward_tunnel(self, local_port, remote_host, remote_port):
        """Create a local tunnel to Oracle via SSH."""

        self._running[local_port] = True  # Flag per controlar el bucle accept

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
                        r, _, _ = select.select([self.sock, self.chan], [], [],0.5)
                        if self.sock in r:
                            try:
                                data = self.sock.recv(1024)
                                if not data:
                                    break
                                if not self.chan.closed:
                                    self.chan.send(data)
                            except (OSError, socket.error):
                                break
                            except:
                                pass

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
            while self._running.get(local_port, False):
                #while True:
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
        t = threading.Thread(target=accept, args=(sock,), daemon=True)
        t.start()
        self._forward_threads[local_port] = t
        self._sockets.append(sock)

        #sshTunnel._local_ports_in_use.add(local_port)
        # només arrenco el monitor si no està actiu
        if not self._monitor_thread or not self._monitor_thread.is_alive():
            self._monitor_thread = self._monitor_ports(interval=5.0)

    def add_forward(self, remote, local=("localhost", 0)):
        """
      Afegeix un nou forward al túnel SSH existent.
      remote: (remote_host, remote_port)
      local: (local_host, local_port)
      """
        remote_host, remote_port = remote
        local_host, local_port = local

        # Verificar si ja tenim aquest mapping
        if (remote_host, remote_port) in self.remote_bind_addresses and (
            local_host, local_port) in self.local_bind_addresses:
            print(f"[WARN] Forward {local_host}:{local_port} -> {remote_host}:{remote_port} ja existeix.")
            return

        try:
            # Crear el forward
            self.forward_tunnel(local_port, remote_host, remote_port)

            # Guardar el mapping
            self.remote_bind_addresses.append((remote_host, remote_port))
            self.local_bind_addresses.append((local_host, local_port))

            print(f"[INFO] Afegit forward {local_host}:{local_port} -> {remote_host}:{remote_port}")
        except Exception as e:
            print(
                f"[ERROR] No s'ha pogut afegir el forward {local_host}:{local_port} -> {remote_host}:{remote_port}: {e}")

    def remove_forward(self, local_port):
        """
        Elimina el forward associat a un port local concret.
        """

        # 1. Senyalitza al forward que s'aturi
        self._running.pop(local_port, None)

        # 2. Tanca el socket associat
        sock_to_close = None
        for sock in self._sockets:
            if sock.getsockname()[1] == local_port:
                sock_to_close = sock
                break

        if sock_to_close:
            sock_to_close.close()
            self._sockets.remove(sock_to_close)

        idx_to_remove = None

        # 2. Espera que tots els threads acabin
        thread = self._forward_threads[local_port]
        if thread.is_alive():
            thread.join()

        # Buscar quin forward correspon al local_port
        for idx, (local_host, lp) in enumerate(self.local_bind_addresses):
            if lp == local_port:
                idx_to_remove = idx
                break

        if idx_to_remove is None:
            print(f"[WARN] No s'ha trobat cap forward per al port local {local_port}")
            return

        # Tancar el socket associat
        try:
            sock_to_remove = self._sockets[idx_to_remove]
            sock_to_remove.close()


            print(f"[INFO] Forward al port local {local_port} eliminat correctament.")
        except Exception as e:
            print(f"[ERROR] No s'ha pogut eliminar el forward al port {local_port}: {e}")

        # Eliminar de les llistes
        try:
            self._sockets.pop(idx_to_remove)
            self.local_bind_addresses.pop(idx_to_remove)
            self.remote_bind_addresses.pop(idx_to_remove)
        except Exception:
            pass

    def start(self):
        self.client = paramiko.SSHClient()
        self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
        self.client.connect(self.ssh_host, port=self.ssh_port, username=self.ssh_username,
                            password=self.ssh_password, key_filename=self.ssh_pkey)
        self.transport = self.client.get_transport()

        for local_bind, remote_bind in zip(self.local_bind_addresses, self.remote_bind_addresses):
            self.forward_tunnel(local_bind[1], remote_bind[0], remote_bind[1])

        # només arrenco el monitor si no està actiu
        if not self._monitor_thread or not self._monitor_thread.is_alive():
            self._monitor_thread = self._monitor_ports(interval=5.0)

    def stop(self):
        #self._running = False  # Atura el bucle accept
        self._stop_all_threads()
        #for sock in self._sockets:
        #    sock.close()
        #self._sockets.clear()
        self._running = {}

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

    def _monitor_ports(self, interval: float = 5.0):
        """
        Llança un thread que comprova periòdicament els ports en ús.
        Si un port està lliure, el treu del set.
        El thread s'atura automàticament si el set queda buit.
        """

        def _monitor():
            while sshTunnel._local_ports_in_use:
                to_remove = {p for p in list(sshTunnel._local_ports_in_use) if _is_port_free(p)}
                if to_remove:
                    sshTunnel._local_ports_in_use.difference_update(to_remove)
                time.sleep(interval)

        t = threading.Thread(target=_monitor, daemon=True)
        t.start()
        return t

    def _stop_all_threads(self):
        """Atura tots els forwards i espera que els threads acabin."""
        # 1. Senyalitza als forwards que s'aturin
        for port in list(self._forward_threads.keys()):
            self._running.pop(port, None)  # el thread sortirà del bucle
            sock = self._sockets.pop(port, None)
            if sock:
                sock.close()

        # 2. Espera que tots els threads acabin
        for thread in self._forward_threads.values():
            if thread.is_alive():
                thread.join()

        # 3. Neteja el diccionari de threads
        self._forward_threads.clear()

        print("Tots els forwards han estat aturats.")


def get_free_port():
    """Retorna un port lliure que no s'hagi utilitzat abans."""
    while True:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.bind(("", 0))  # 0 -> que el sistema triï un port lliure
            port = s.getsockname()[1]
        if port not in sshTunnel._local_ports_in_use:
            sshTunnel._local_ports_in_use.add(port)
            return port

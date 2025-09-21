# -*- coding: utf-8 -*-
u"""
Created on Jun 12, 2018

"""

import gc
import select
import socket
import threading
import time
import logging
from contextlib import closing
from typing import Tuple, List, Optional, Dict, Any
import paramiko

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


def is_port_available(port, host="localhost") -> bool:
    """Check if a port is available for binding."""
    try:
        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            s.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            s.bind((host, port))
            return True
    except OSError:
        return False


def get_free_port(host: str = "localhost") -> int:
    """Retorna un port lliure que no s'hagi utilitzat abans."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind((host, 0))
        return s.getsockname()[1]


class TunnelHandler(threading.Thread):
    """Handles data forwarding between local socket and SSH channel."""

    def __init__(self, channel, local_socket):
        super().__init__(daemon=True)
        self.channel = channel
        self.local_socket = local_socket
        self._running = True

    def stop(self):
        self._running = False

    def run(self):
        try:
            while self._running:
                # Use select with timeout to allow clean shutdown
                ready, _, _ = select.select([self.local_socket, self.channel], [], [], 0.5)

                if not ready:
                    continue

                if self.local_socket in ready:
                    try:
                        data = self.local_socket.recv(4096)  # Increased buffer size
                        if not data:
                            break
                        if not self.channel.closed:
                            self.channel.send(data)
                    except (OSError, socket.error) as e:
                        logger.debug(f"Local socket error: {e}")
                        break

                if self.channel in ready:
                    try:
                        data = self.channel.recv(4096)  # Increased buffer size
                        if not data:
                            break
                        self.local_socket.send(data)
                    except (OSError, socket.error) as e:
                        logger.debug(f"Channel error: {e}")
                        break

        except Exception as e:
            logger.error(f"Handler error: {e}")
        finally:
            self._cleanup()

    def _cleanup(self):
        """Clean up resources."""
        try:
            if not self.channel.closed:
                self.channel.close()
        except Exception as e:
            logger.debug(f"Error closing channel: {e}")

        try:
            self.local_socket.close()
        except Exception as e:
            logger.debug(f"Error closing socket: {e}")


class ForwardServer(threading.Thread):
    """Manages a single port forward."""

    def __init__(self, transport, local_port: int, remote_host: str, remote_port: int):
        super().__init__(daemon=True)
        self.transport = transport
        self.local_port = local_port
        self.remote_host = remote_host
        self.remote_port = remote_port
        self._running = True
        self._handlers: List[TunnelHandler] = []
        self.server_socket = None

    def stop(self):
        """Stop the forward server and all handlers."""
        self._running = False

        # Stop all handlers
        for handler in self._handlers[:]:  # Copy list to avoid modification during iteration
            handler.stop()

        # Close server socket
        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception as e:
                logger.debug(f"Error closing server socket: {e}")

    def run(self):
        try:
            self.server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            self.server_socket.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
            self.server_socket.settimeout(1.0)  # Set timeout for accept()
            self.server_socket.bind(("localhost", self.local_port))
            self.server_socket.listen(10)

            logger.info(f"Forward server listening on localhost:{self.local_port}")

            while self._running:
                try:
                    client_socket, addr = self.server_socket.accept()
                    if not self._running:
                        client_socket.close()
                        break

                    try:
                        channel = self.transport.open_channel(
                            "direct-tcpip",
                            (self.remote_host, self.remote_port),
                            addr
                        )

                        handler = TunnelHandler(channel, client_socket)
                        self._handlers.append(handler)
                        handler.start()

                        # Clean up finished handlers
                        self._handlers = [h for h in self._handlers if h.is_alive()]

                    except Exception as e:
                        logger.error(f"Error creating channel: {e}")
                        client_socket.close()

                except socket.timeout:
                    continue
                except OSError:
                    if self._running:
                        logger.error(f"Server socket error on port {self.local_port}")
                    break

        except Exception as e:
            logger.error(f"Forward server error: {e}")
        finally:
            self._cleanup()

    def _cleanup(self):
        """Clean up all resources."""
        for handler in self._handlers:
            handler.stop()

        if self.server_socket:
            try:
                self.server_socket.close()
            except Exception:
                pass

    def __str__(self) -> str:
        """Representació amigable del túnel."""
        return f"{self.local_port} <- {self.remote_host}:{self.remote_port}"

    def __repr__(self) -> str:
        """Representació tècnica del túnel."""
        return (f"<ForwardServer local={self.local_port} "
                f"remote={self.remote_host}:{self.remote_port}>")


class SSHTunnel:

    def __init__(self, ssh_host: str, ssh_port: int = 22, ssh_username: Optional[str] = None,
                 ssh_password: Optional[str] = None, ssh_pkey: Optional[str] = None,
                 remote_bind_addresses: Optional[List[Tuple[str, int]]] = None,
                 local_bind_addresses: Optional[List[Tuple[str, int]]] = None):

        self.ssh_host = ssh_host
        self.ssh_port = ssh_port
        self.ssh_username = ssh_username
        self.ssh_password = ssh_password
        self.ssh_pkey = ssh_pkey

        self.remote_bind_addresses = remote_bind_addresses or []
        # self.local_bind_addresses = local_bind_addresses or {}  # TODO :Ha de ser un diccionari on les claus són les addreces \
        # locals i els values el nombre de forwards actius. Aquest valor s'haurà d'incrementar i decrementar segons s'afegeixin o eliminin forwards.

        # Converteix la llista en un diccionari { (host, port): 0 }
        self.local_bind_addresses: Dict[Tuple[str, int], int] = {
            addr: 0 for addr in (local_bind_addresses or [])
        }


        # Ensure we have matching local/remote addresses
        while len(self.local_bind_addresses) < len(self.remote_bind_addresses):
            self.local_bind_addresses.append(("localhost", 0))

        self.client: Optional[paramiko.SSHClient] = None
        self.transport: Optional[paramiko.SSHClient] = None
        self._forward_servers: Dict[int, ForwardServer] = {}
        self._lock = threading.RLock()

    @property
    def local_bind_ports(self) -> List[int]:
        with self._lock:
            return list(self._forward_servers.keys())

    @property
    def local_bind_port(self):
        return self.local_bind_ports[0] if self.local_bind_ports else None

    def add_forward(self, remote_host: str, remote_port: int,
                    local_host: str = "localhost", local_port: int = 0) -> int:
        """
        Afegeix un nou forward al túnel SSH existent.
        remote: (remote_host, remote_port)
        local: (local_host, local_port)
        """

        if not self.transport:
            raise RuntimeError("SSH tunnel not started")

        if local_port == 0:
            local_port = get_free_port(local_host)
        #elif not is_port_available(local_port, local_host):
        #    raise RuntimeError(f"Port {local_port} is not available on {local_host}")

        with self._lock:
            if local_port in self._forward_servers:
                server = self._forward_servers[local_port]
                if not (server.remote_host == remote_host and server.remote_port == remote_port):
                    raise RuntimeError(f"Port {local_port} is already forwarded")

                # Incrementem comptador de forwards existents
                self.local_bind_addresses[(local_host, local_port)] = (
                    self.local_bind_addresses.get((local_host, local_port), 0) + 1
                )
                return local_port  # Ja estava endreçat al mateix remote

            else:
                actual_port = self._start_forward(local_port, remote_host, remote_port)

                # Guardar el mapping
                self.remote_bind_addresses.append((remote_host, remote_port))
                # self.local_bind_addresses.append((local_host, actual_port))

                # Inicialitzar o incrementar comptador en el diccionari
                self.local_bind_addresses[(local_host, actual_port)] = (
                    self.local_bind_addresses.get((local_host, actual_port), 0) + 1
                )

                logger.info(f"Added forward {local_host}:{actual_port} -> {remote_host}:{remote_port}")
                return actual_port

    def remove_forward(self, local_port: int):
        """
        Elimina el forward associat a un port local concret.
        """

        with self._lock:
            if local_port not in self._forward_servers:
                raise RuntimeError(f"No forward exists for local port {local_port}")

        keys_to_remove = set()
        for (host, port), count in self.local_bind_addresses.items():
            if port == local_port:
                if count > 1:
                    # Només decrementem
                    self.local_bind_addresses[(host, port)] = count - 1
                    logger.info(f"Decremented forward count for {host}:{port} -> {count - 1} active")
                    return
                else:
                    # Marquem per eliminar del diccionari i del servidor
                    keys_to_remove.add((host, port) )


        # Si el comptador era 1, eliminem el servidor i el mapping
        for key_to_remove in keys_to_remove:
            server = self._forward_servers.pop(local_port)
            server.stop()

            self.local_bind_addresses.pop(key_to_remove, None)

            for i, (rhost, rport) in enumerate(self.remote_bind_addresses):
                if key_to_remove[1] == local_port:
                    self.remote_bind_addresses.pop(i)
                    break

        logger.info(f"Removed forward for port {local_port}")

    def _start_forward(self, local_port: int, remote_host: str, remote_port: int) -> int:
        """Start a single forward server."""
        server = ForwardServer(self.transport, local_port, remote_host, remote_port)
        server.start()

        self._forward_servers[local_port] = server
        return local_port

    def start(self):
        """Start the SSH tunnel."""
        if self.client:
            logger.warning("Tunnel already started")
            return

        try:
            self.client = paramiko.SSHClient()
            self.client.set_missing_host_key_policy(paramiko.AutoAddPolicy())
            self.client.connect(
                self.ssh_host,
                port=self.ssh_port,
                username=self.ssh_username,
                password=self.ssh_password,
                key_filename=self.ssh_pkey,
                timeout=10)

            self.transport = self.client.get_transport()
            logger.info(f"Connected to SSH server {self.ssh_host}:{self.ssh_port}")

            for local_addr, remote_addr in zip(self.local_bind_addresses, self.remote_bind_addresses):
                local_host, local_port = local_addr
                remote_host, remote_port = remote_addr

                # Get actual port if 0 was specified
                if local_port == 0:
                    local_port = get_free_port(local_host)

                self._start_forward(local_port, remote_host, remote_port)

        except Exception as e:
            logger.error(f"Failed to start SSH tunnel: {e}")
            self.stop()
            raise

    def stop(self):
        """Stop the SSH tunnel and clean up resources."""
        logger.info("Stopping SSH tunnel...")

        with self._lock:
            for server in list(self._forward_servers.values()):
                server.stop()

            self._forward_servers.clear()

        if self.transport:
            try:
                self.transport.close()
            except Exception as e:
                logger.debug(f"Error closing transport: {e}")
            finally:
                self.transport = None

        if self.client:
            try:
                self.client.close()
            except Exception as e:
                logger.debug(f"Error closing client: {e}")
            finally:
                self.client = None

        gc.collect()
        logger.info("SSH tunnel stopped")

    def __enter__(self):
        self.start()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.stop()

    def is_active(self, timeout=2):
        if not self.transport or not self.transport.active:
            return False

        # Test one of the local ports
        for local_port in self.local_bind_ports:
            try:
                with closing(socket.create_connection(("localhost", local_port), timeout=timeout)):
                    return True
            except Exception:
                continue

        return False

    def __getitem__(self, index: int):
        """
        Accés per índex als servidors de forwards.
        """
        return list(self._forward_servers.values())[index]

    def __len__(self):
        """Nombre de forwards actius."""
        return len(self._forward_servers)

    def __iter__(self):
        """Iterador sobre els servidors de forwards."""
        return iter(self._forward_servers.values())

    def pop(self, key: int) -> Optional[Any]:
        """
        Elimina i retorna el servidor de forward associat a un port local.
        :param key: port local
        :return: el servidor eliminat o None si no existeix
        """
        return self._forward_servers.pop(key, None)

    def __str__(self) -> str:
        """Representació amigable del túnel amb forwards."""
        base = f"{self.ssh_username}@{self.ssh_host}:{self.ssh_port}"
        if not self._forward_servers:
            return f"{base} (sense forwards actius)"
        forwards = ", ".join(str(fwd) for fwd in self._forward_servers.values())
        return f"{base} -> [{forwards}]"

    def __repr__(self) -> str:
        """Representació tècnica per a debugging."""
        return (f"<SSHTunnel user={self.ssh_username} host={self.ssh_host} "
                f"port={self.ssh_port} forwards={len(self._forward_servers)}>")

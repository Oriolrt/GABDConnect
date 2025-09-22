# -*- coding: utf-8 -*-
u"""
Created on Jun 12, 2018

@author: Oriol Ramos Terrades
@email: oriol.ramos@uab.cat

copyright: 2018, Oriol Ramos Terrades

Aquest script forma part del material didàctic de l'assignatura de Gestió i Administració de Bases de Dades (GABD) de la
Universitat Autònoma de Barcelona. Les classes `AbsConnection` i `GABDSSHTunnel` proporcionen una base per a la gestió
de connexions a bases de dades i la configuració de túnels SSH, respectivament. Aquestes eines són essencials per a
l'administració segura i eficient de bases de dades en entorns distribuïts.
"""

import warnings
from abc import ABC, abstractmethod
from .ssh_tunnel import SSHTunnel, get_free_port
from typing import Optional, Any, Union

from getpass import getpass


warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module="pydevd"
)


def _format_multiple_tunnels(mt: dict) -> dict:
    res = dict()
    for k, v in mt.items():
        kk = int(k)
        if isinstance(v, str):
            vv = v.split(':')
            vv = (str(vv[0]), int(vv[1]))
        elif isinstance(v, tuple) and len(v) == 2:
            vv = v
        else:
            return dict()
        res[kk] = vv

    return res


class GABDSSHTunnel:
    """
      Classe per gestionar túnels SSH per a connexions a bases de dades.
      """
    _servers = {}  # clau = (ssh, port, user), valor = sshTunnel
    _num_connections = 0

    __slots__ = ['_hostname', '_port', '_ssh_data', '_local_port', '_mt', '_context_mode']

    def __init__(self, hostname, port, ssh_data=None, **kwargs):
        """
            Constructor per inicialitzar el túnel SSH amb els paràmetres donats.

            Paràmetres:
            -----------
            hostname : str
                Nom de l'host o adreça IP del servidor SSH.
            port : int
                Port del servidor SSH.
            ssh_data : dict, opcional
                Informació d'autenticació SSH.
        """

        self._context_mode = None  # "tunnel" o "session"
        self._hostname = hostname
        if port is not None and (not isinstance(port, int) or port < 1 or port > 65535):
            raise ValueError(f"Port '{port}' no vàlid. Ha de ser un enter entre 1 i 65535")
        elif port is None:
            raise ValueError("Port no pot ser None")

        self._port = port

        self._ssh_data = ssh_data
        if 'multiple_tunnels' in kwargs:
            self._mt = a = _format_multiple_tunnels(kwargs['multiple_tunnels'].copy())
            try:
                self._local_port = int(kwargs.pop('local_port', {v[0]: k for k, v in a.items()}[self.hostname]))
            except KeyError:
                self._local_port = get_free_port()
                raise KeyError(
                    f"""No s'ha definit un port local per redireccionar {self.hostname}:{self._port}. 
                    S'agafarà {self._port} per defecte.""")

            self._mt[self._local_port] = (self._hostname, self._port)
        else:
            self._local_port = int(kwargs.pop('local_port', get_free_port()))
            self._mt = _format_multiple_tunnels({self._local_port: (hostname, port)})

    @property
    def ssh(self):
        return self._ssh_data

    @ssh.setter
    def ssh(self, valor: dict):
        self._ssh_data = valor

    @property
    def server(self):
        return self._servers

    @server.setter
    def server(self, value):
        _servers = value

    @property
    def hostname(self):
        return self._hostname

    @hostname.setter
    def hostname(self, valor: str):
        self._hostname = valor

    @property
    def port(self):
        return self._port

    @port.setter
    def port(self, valor: str):
        self._port = valor

    def __enter__(self):
        self.opentunnel()
        return self

    def __exit__(self, exc_type, exc_value, traceback):
        self.closetunnel()

    def opentunnel(self) -> bool:
        """
          Obre un túnel SSH utilitzant la informació d'autenticació proporcionada.

          Retorna:
          --------
          bool
          """

        if self._mt is not None:
            remote_binds = [(remote_host, remote_port) for _, (remote_host, remote_port) in self._mt.items()]
            local_binds = [("", local_port) for local_port in self._mt.keys()]
        else:
            remote_binds = [(self._hostname, int(self._port))]
            local_binds = [("", int(self._local_port))]

        if self._ssh_data is None:
            raise ValueError("Falten dades SSH")

        ssh_data = self._ssh_data
        key = (ssh_data["ssh"], int(ssh_data["port"]), ssh_data["user"])

        # Comprovar si ja existeix el túnel per aquest host
        if key not in GABDSSHTunnel._servers:
            # Autenticació
            if "id_key" in ssh_data:
                tunnel = SSHTunnel(
                    ssh_data["ssh"],
                    ssh_port=int(ssh_data['port']),
                    ssh_username=ssh_data["user"],
                    ssh_pkey=ssh_data["id_key"],
                    remote_bind_addresses=[],
                    local_bind_addresses=[]
                )
            else:
                if "pwd" not in ssh_data or not ssh_data["pwd"]:
                    ssh_data["pwd"] = getpass(
                        prompt=f"Password de l'usuari {ssh_data['user']} a {ssh_data['ssh']}: "
                    )
                tunnel = SSHTunnel(
                    ssh_data["ssh"],
                    ssh_port=int(ssh_data['port']),
                    ssh_username=ssh_data["user"],
                    ssh_password=ssh_data["pwd"],
                    remote_bind_addresses=[],
                    local_bind_addresses=[]
                )

            # Crear connexió SSH
            try:
                tunnel.start()
                GABDSSHTunnel._servers[key] = tunnel
                GABDSSHTunnel._num_connections += 1
                print(f"[INFO] Connexió SSH oberta a {ssh_data['ssh']}:{ssh_data['port']} com {ssh_data['user']}")
            except Exception as e:
                print(f"[ERROR] No s'ha pogut obrir el túnel: {e}")
                return False

        # Afegir forwards (tant si és túnel nou com si ja existia)
        tunnel = GABDSSHTunnel._servers[key]
        for r, l in zip(remote_binds, local_binds):
            tunnel.add_forward(*r, *l)

        # Missatge d'info
        if self._mt is not None:
            forwards = " -L ".join([f"{local_port}:{remote_host}:{remote_port}"
                                    for local_port, (remote_host, remote_port) in self._mt.items()])
        else:
            forwards = f"{self._local_port}:{self._hostname}:{self._port}"

        print(f"ssh -L {forwards} {ssh_data['user']}@{ssh_data['ssh']} -p {ssh_data['port']}")

        return True

    def closetunnel(self) -> Optional[bool]:
        """
          Tanca el forward associat a aquesta connexió Oracle.
          Si és l'últim forward d'un túnel SSH, tanca també el túnel.
          """

        tunnel = self.get_tunnel()

        if not tunnel:
            print("[WARN] No s'ha trobat cap túnel actiu per tancar")
            return

        # Determinar quin port local s'estava utilitzant
        if self._mt is not None:
            local_ports = list(self._mt.keys())
        else:
            local_ports = [int(self._local_port)]
            # TODO: Això ha de ser un diccionari on els values són el nombre de connexions \
            # que utilitzen aquest port local

        # Eliminar forwards d'aquest objecte
        for lp in local_ports:
            # TODO: s'ha de decrementar en un el nombre de connexions que utilitzen el local_port local i si \
            # arriba a 0, eliminar-lo
            tunnel.remove_forward(lp)

        # Si no queden forwards, tanquem completament el túnel
        if not tunnel.local_bind_addresses:
            tunnel.stop()
            GABDSSHTunnel.pop(tunnel)
            print(f"[INFO] Túnel SSH {tunnel} tancat (sense forwards restants).")
        else:
            print(f"[INFO] Forwards {local_ports} eliminats, túnel SSH segueix actiu amb altres forwards.")

        return True

    def is_active(self) -> bool:
        """Retorna si el túnel SSH està actiu."""
        if self._ssh_data is None:
            return False

        ssh_data = self._ssh_data
        key = (ssh_data["ssh"], int(ssh_data["port"]), ssh_data["user"])

        tunnel = GABDSSHTunnel._servers.get(key)
        return tunnel.is_active() if tunnel else False

    def _make_key(self):
        """Construeix la clau (ssh, port, user) a partir de self._ssh_data."""
        if self._ssh_data is None:
            print("[WARN] No hi ha dades SSH per construir la clau")
            return None
        return (
            self._ssh_data["ssh"],
            int(self._ssh_data["port"]),
            self._ssh_data["user"],
        )

    def get_tunnel(self):
        """Retorna el túnel associat a self._ssh_data, si existeix."""
        key = self._make_key()
        if key is None:
            return None
        return GABDSSHTunnel._servers.get(key)

    @classmethod
    def get(cls, ssh: str, port: int, user: str):
        """Accedeix al túnel actiu amb clau (ssh, port, user)."""
        key = (ssh, int(port), user)
        return cls._servers.get(key)




    def __delitem__(self, key):
        """
        Permet eliminar un túnel amb del t[ssh, port, user].
        """
        if not isinstance(key, tuple) or len(key) != 3:
            raise KeyError("La clau ha de ser (ssh, port, user)")
        ssh, port, user = key
        GABDSSHTunnel._servers.pop((ssh, int(port), user), None)

    def __contains__(self, key):
        """
        Permet comprovar si un túnel existeix amb (ssh, port, user) in t.
        """
        if not isinstance(key, tuple) or len(key) != 3:
            return False
        ssh, port, user = key
        return (ssh, int(port), user) in GABDSSHTunnel._servers

    def __getitem__(self, key):
        if isinstance(key, int):
            # Accés per índex com si fos una llista
            return list(self._servers.values())[key]
        elif isinstance(key, tuple) and len(key) == 3:
            # Accés per clau (ssh, port, user)
            ssh, port, user = key
            return self._servers.get((ssh, int(port), user))
        else:
            raise KeyError("Ús invàlid: utilitza un int o una tupla (ssh, port, user)")

    def __len__(self):
        """Retorna el nombre de túnels actius."""
        return len(self._servers)

    def __iter__(self):
        """Permet iterar directament sobre els túnels."""
        return iter(self._servers.values())

    @classmethod
    def pop(cls, item):
        """
        Elimina un túnel de _servers.
        :param item: SSHTunnel o clau (ssh, port, user)
        :return: el túnel eliminat o None si no existeix
        """
        key = None

        # Si és una instància de SSHTunnel, trobem la clau corresponent
        if isinstance(item, SSHTunnel):
            for k, v in cls._servers.items():
                if v is item:
                    key = k
                    break
            if key is None:
                return None  # No trobat

        # Si és una tupla, la fem servir com a clau
        elif isinstance(item, tuple) and len(item) == 3:
            key = (item[0], int(item[1]), item[2])

        else:
            raise ValueError("El paràmetre ha de ser SSHTunnel o clau (ssh, port, user)")

        # Eliminar i decrementar el comptador
        removed: Union[Any, None] = cls._servers.pop(key, None)
        if removed is not None:
            cls._num_connections -= 1
        return removed

    @classmethod
    def close_all_tunnels(cls):
        """
        Tanca tots els túnels SSH actius.
        """
        cls._servers.clear()


class AbsConnection(ABC, GABDSSHTunnel):
    """
    Aquesta classe abstracta emmagatzema informació bàsica de connexió i mètodes per connectar-se a DBMS.
    """

    __slots__ = ['_conn', '_is_started', '_user', '_pwd']

    def __init__(self, **params):
        """
        Constructor per inicialitzar la connexió amb els paràmetres donats.

        Paràmetres:
        -----------
        **params : dict
            Paràmetres de connexió, incloent `user`, `passwd`, `hostname` i `port`.
        """

        self._conn = None
        self._is_started = False
        self._user = params.pop('user', None)
        self._pwd = params.pop('passwd', None)
        # self._bd = params.pop('bd', None)
        hostname = params.pop('hostname', 'localhost')
        port = params.pop('port', None)

        GABDSSHTunnel.__init__(self, hostname, port, **params)

    @property
    def conn(self):
        return self._conn

    @conn.setter
    def conn(self, valor):
        self._conn = valor
        self._is_started = True

    @property
    def server(self):
        ssh_data = self._ssh_data
        key = (ssh_data["ssh"], int(ssh_data["port"]), ssh_data["user"])
        return self._servers[key] if key in self._servers else None

    @property
    def is_started(self):
        return self._is_started

    @is_started.setter
    def is_started(self, valor: bool):
        self._is_started = valor

    @property
    def isStarted(self) -> bool:
        warnings.warn(
            "isStarted està obsolet i s'eliminarà en futures versions. "
            "Fes servir is_started en el seu lloc.",
            DeprecationWarning,
            stacklevel=2
        )
        return self._is_started

    @isStarted.setter
    def isStarted(self, valor: bool):
        warnings.warn(
            "isStarted està obsolet i s'eliminarà en futures versions. "
            "Fes servir is_started en el seu lloc.",
            DeprecationWarning,
            stacklevel=2
        )
        self._is_started = valor

    @property
    def user(self):
        return self._user

    @user.setter
    def user(self, valor: str):
        self._user = valor

    @property
    def pwd(self):
        return self._pwd

    @pwd.setter
    def pwd(self, valor: str):
        self._pwd = valor

    def __enter__(self):
        self.open()
        self._context_mode = "tunnel"
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Tanca la connexió Oracle quan es surt del context manager.
        """
        if self._context_mode == "tunnel":
            self.close()
        elif self._context_mode == "session":
            self.close_session()
        self._context_mode = None  # netegem
        # return False  # no suprimim excepcions


    def __str__(self):
        return f"Connexió a {self._hostname}:{self._port} amb l'usuari {self._user} a la base de dades "  # \
#        {self._bd if self._bd is not None else '.'}"

    def __repr__(self):
        return f"Connexió a {self._hostname}:{self._port} amb l'usuari {self._user} a la base de dades "  # \
#        {self._bd if self._bd is not None else '.'}"

    def __getitem__(self, item):
        return self.__getattribute__(item)

    def __setitem__(self, key, value):
        self.__setattr__(key, value)

    @abstractmethod
    def open(self):
        """
        Connecta a un servidor DBMS amb la informació de connexió guardada.

        Retorna:
        --------
        self
        """

        super().opentunnel()  # Obre el túnel SSH

        self._is_started = self.is_active()

        self._context_mode = "session"
        return self

    @abstractmethod
    def close(self):
        """
        Tanca la connexió al servidor DBMS.

        Retorna:
        --------
        None
        """
        self._is_started = False

    def commit(self):
        """
          Fa un commit de la transacció actual.

          Retorna:
          --------
          None
        """
        pass

    @abstractmethod
    def test_connection(self):
        """
          Prova la connexió al servidor DBMS.

          Retorna:
          --------
          bool
              True si la connexió és correcta, False en cas contrari.
        """
        pass

    def testConnection(self):
        warnings.warn(
            f"testConnectiond està obsolet i s'eliminarà en futures versions. "
            "Fes servir test_connection en el seu lloc.",
            DeprecationWarning,
            stacklevel=2
        )
        return self.test_connection()


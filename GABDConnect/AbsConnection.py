# -*- coding: utf-8 -*-
u"""
Created on Jun 12, 2018

@author: Oriol Ramos Terrades
@email: oriol.ramos@uab.cat

copyrigth: 2018, Oriol Ramos Terrades

Aquest script forma part del material didàctic de l'assignatura de Gestió i Administració de Bases de Dades (GABD) de la Universitat Autònoma de Barcelona. Les classes `AbsConnection` i `GABDSSHTunnel` proporcionen una base per a la gestió de connexions a bases de dades i la configuració de túnels SSH, respectivament. Aquestes eines són essencials per a l'administració segura i eficient de bases de dades en entorns distribuïts.
"""

from abc import ABC, abstractmethod

import warnings
warnings.filterwarnings(
    "ignore",
    category=DeprecationWarning,
    module="pydevd"
)


from .ssh_tunnel import sshTunnel
#from sshtunnel import SSHTunnelForwarder as sshTunnel

from getpass import getpass

def _format_multiple_tunnels( mt : dict) -> bool:
  res = dict()
  for k,v in mt.items():
    kk = int(k)
    if  isinstance(v,str):
      vv = v.split(':')
      vv = ( str(vv[0]), int(vv[1]))
    elif isinstance(v,tuple) and len(v)==2:
      vv = v
    else:
      return None
    res[kk] = vv

  return res

class GABDSSHTunnel:
    """
    Classe per gestionar túnels SSH per a connexions a bases de dades.
    """
    _server = None
    _num_connections = 0

    __slots__ = ['_hostname', '_port', '_ssh_data','_local_port','_mt']
    def __init__(self, hostname, port, ssh_data=None,**kwargs):
        '''
        Constructor per inicialitzar el túnel SSH amb els paràmetres donats.

        Paràmetres:
        -----------
        hostname : str
            Nom de l'host o adreça IP del servidor SSH.
        port : int
            Port del servidor SSH.
        ssh_data : dict, opcional
            Informació d'autenticació SSH.
        '''
        self._hostname = hostname
        self._port = port if port is not None else 22
        self._local_port = int(kwargs.pop('local_port',self._port))
        self._ssh_data = ssh_data
        if 'multiple_tunnels' in kwargs:
          self._mt = kwargs['multiple_tunnels'].copy()
          self._mt[self._local_port] = (self._hostname, self._port)
          self._mt = _format_multiple_tunnels(self._mt)
        else:
          self._mt = None


    @property
    def ssh(self):
      return self._ssh_data

    @ssh.setter
    def ssh(self, valor: dict):
      self._ssh_data = valor

    @property
    def server(self):
        return self._server

    @server.setter
    def server(self, value):
        _server = value

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

    def openTunnel(self):
      """
      Obre un túnel SSH utilitzant la informació d'autenticació proporcionada.

      Retorna:
      --------
      None
      """

      if self._mt is not None:
        remote_binds = [(remote_host, remote_port) for _, (remote_host, remote_port) in self._mt.items()]
        local_binds = [("", local_port) for local_port in self._mt.keys()]
      else:
        remote_binds = [(self._hostname, int(self._port))]
        local_binds = [("", int(self._local_port))]


      if self._ssh_data is not None:
        ssh_data = self._ssh_data
        if ssh_data is not None:
          if "id_key" in ssh_data:
            GABDSSHTunnel._server = sshTunnel(
                (ssh_data["ssh"], int(ssh_data['port'])),
                ssh_username=ssh_data["user"],
                ssh_pkey=ssh_data["id_key"],
                remote_bind_addresses=remote_binds,
                local_bind_addresses=local_binds
            )
          else:
            if "pwd" in ssh_data:
              if ssh_data["pwd"] == "" or ssh_data["pwd"] is None:
                ssh_data["pwd"] = getpass(prompt="Password de l'usuari {} a {}: ".format(ssh_data["user"], ssh_data["ssh"]))
            else:
              ssh_data["pwd"] = getpass(prompt="Password de l'usuari {} a {}: ".format(ssh_data["user"], ssh_data["ssh"]))

            GABDSSHTunnel._server = sshTunnel(
                (ssh_data["ssh"], int(ssh_data['port'])),
                ssh_username=ssh_data["user"],
                ssh_password=ssh_data["pwd"],
                remote_bind_addresses=remote_binds,
                local_bind_addresses=local_binds
            )

          if self._mt is not None:
            forwards = " -L ".join([f"{local_port}:{remote_host}:{remote_port}" for local_port, (remote_host, remote_port) in self._mt.items()])
          else:
            forwards = f"  {self._local_port}:{self._hostname}:{self._port}"


          if GABDSSHTunnel._num_connections == 0:
            try:
              GABDSSHTunnel._server.start()
              GABDSSHTunnel._num_connections += 1
              message = f"Connexió SSH a {self._hostname} oberta. S'ha obert un túnel a través de {ssh_data['ssh']} " \
                        f"al port {self._port}. La instrucció equivalent per fer-ho manualment seria: \n" \
                        f"ssh -L {forwards} {ssh_data['user']}@{ssh_data['ssh']} -p {ssh_data['port']}"
              print(message)

            except Exception as e:
              print(f"Error al obrir el túnel SSH: {e}")



    def closeTunnel(self):
        """
        Tanca el túnel SSH obert.

        Retorna:
        --------
        None
        """
        if GABDSSHTunnel._server is not None:
            if GABDSSHTunnel._num_connections > 0:
                GABDSSHTunnel._num_connections -= 1
            if GABDSSHTunnel._num_connections == 0:
              GABDSSHTunnel._server.stop()
              GABDSSHTunnel._server = None
            print(f"Connexió SSH a {self._hostname} tancada.")

class AbsConnection(ABC,  GABDSSHTunnel):
  """
  Aquesta classe abstracta emmagatzema informació bàsica de connexió i mètodes per connectar-se a DBMS.
  """

  __slots__ = ['_conn',  '_isStarted', '_user','_pwd']

  def __init__(self,**params):
    '''
    Constructor per inicialitzar la connexió amb els paràmetres donats.

    Paràmetres:
    -----------
    **params : dict
        Paràmetres de connexió, incloent `user`, `passwd`, `hostname` i `port`.
    '''

    self._conn = None
    self._isStarted = False
    self._user = params.pop('user', None)
    self._pwd = params.pop('passwd',None)
    hostname = params.pop('hostname', 'localhost')
    port = params.pop('port', None)

    GABDSSHTunnel.__init__(self, hostname, port, **params)


  @property
  def conn(self):
    return self._conn

  @conn.setter
  def conn(self, valor):
    self._conn = valor
    self._isStarted = True

  @property
  def server(self):
    return self._server

  @server.setter
  def server(self, server : object):
    self._server = server

  @property
  def isStarted(self):
    return self._isStarted

  @isStarted.setter
  def isStarted(self, valor : bool):
    self._isStarted = valor

  @property
  def user(self):
    return self._user

  @user.setter
  def user(self, valor : str):
    self._user = valor


  @property
  def pwd(self):
    return self._pwd

  @pwd.setter
  def pwd(self, valor : str):
    self._pwd = valor

  def __str__(self):
    return f"Connexió a {self._hostname}:{self._port} amb l'usuari {self._user} a la base de dades {self._bd if self._bd is not None else '.'}"

  def __repr__(self):
    return f"Connexió a {self._hostname}:{self._port} amb l'usuari {self._user} a la base de dades {self._bd if self._bd is not None else '.'}"

  def  __getitem__(self, item):
    return self.__getattribute__(item)

  def __setitem__(self, key, value):
    self.__setattr__(key, value)

  @abstractmethod
  def open(self):
    """
    Connecta a un servidor DBMS amb la informació de connexió guardada.

    Retorna:
    --------
    None
    """

    super().openTunnel()  # Obre el túnel SSH

    self._isStarted = True

    return self._isStarted

  @abstractmethod
  def close(self):
    """
    Tanca la connexió al servidor DBMS.

    Retorna:
    --------
    None
    """
    self._isStarted = False

  def commit(self):
    """
      Fa un commit de la transacció actual.

      Retorna:
      --------
      None
    """
    pass

  @abstractmethod
  def testConnection(self):
    """
      Prova la connexió al servidor DBMS.

      Retorna:
      --------
      bool
          True si la connexió és correcta, False en cas contrari.
    """
    pass

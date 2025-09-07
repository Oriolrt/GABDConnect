# -*- coding: utf-8 -*-
u"""
Created on Jun 12, 2018

@author: Oriol Ramos Terrades
@email: oriol.ramos@uab.cat

copyrigth: 2018, Oriol Ramos Terrades

Aquest script forma part del material didàctic de l'assignatura de Gestió i Administració de Bases de Dades (GABD) de la Universitat Autònoma de Barcelona. La classe `oracleConnection` proporciona una implementació específica per a la gestió de connexions a bases de dades Oracle, incloent la configuració i manteniment de les connexions. Aquesta eina és essencial per a l'administració segura i eficient de bases de dades Oracle en entorns distribuïts.
"""


from getpass import getpass
import logging
from oracledb import *

from .AbsConnection import AbsConnection


class oracleConnection(AbsConnection):
  '''
  Classe per gestionar la connexió a una base de dades Oracle.

  Atributs:
  ----------
  _cursor : oracledb.Cursor
      Cursor per executar procediments i consultes.
  _serviceName : str
      Nom del servei de la base de dades.
  _dsn : str
      Data Source Name per a la connexió a la base de dades.
  '''

  __slots__ = ['_cursor','_serviceName','_dsn']

  def __init__(self, **params):
    '''
    Constructor per inicialitzar la connexió Oracle.

    Paràmetres:
    -----------
    **params : dict
        Paràmetres de connexió, incloent `serviceName` i `port`.
     '''

    self._cursor = None
    self._serviceName = params.pop('serviceName', 'orcl')
    params['port'] = params.pop('port', 1521)


    AbsConnection.__init__(self,**params)

    self._dsn = f"{self.user}/{self.pwd}@localhost:{self._local_port}/{self._serviceName}"



  def cursor(self) -> DB_TYPE_CURSOR:
    '''
    Retorna el cursor de la connexió Oracle.

    Retorna:
    --------
    oracledb.Cursor
        El cursor de la connexió.
    '''
    try:
      self._cursor = self.conn.cursor()
      self._cursor.callproc("dbms_output.enable")
      return self._cursor
    except DatabaseError:
      logging.warning('Database connection already closed')


  def open(self) -> None:
    """
      Connect to a oracle server given the connexion information saved on the cfg member variable.

      :return: None
    """

    AbsConnection.open(self)

    try:
      self.conn = connect(self._dsn)
      self._cursor = self.conn.cursor()
      self.isStarted = True
    except DatabaseError as e:
      self.closeTunnel()
      self.isStarted = False
      logging.error(f"Error connecting to the database with dsn: {self._dsn}")
      logging.error(f"Error: {e}")


  def close(self) -> None:
    '''
    Tanca la connexió a la base de dades Oracle.

    Retorna:
    --------
    None
    '''
    try:
      self.conn.close()
      self.closeTunnel()
      self.isStarted = False
    except DatabaseError:
      logging.warning('Database connection already closed')


  def commit(self) -> None:
    '''
    Fa un commit de la transacció actual.

    Retorna:
    --------
    None
    '''
    self.conn.commit()

  def testConnection(self) -> bool:
    '''
    Prova la connexió a la base de dades Oracle.

    Retorna:
    --------
    bool
        True si la connexió és correcta, False en cas contrari.
    '''
    cur = self._cursor

    res = cur.execute("""SELECT sys_context('USERENV','SESSION_USER')  as "CURRENT USER" ,
                      sys_context('USERENV', 'CURRENT_SCHEMA') as "CURRENT SCHEMA"
                      FROM dual""").fetchone()

    print("Current user: {}, Current schema: {}".format(res[0], res[1]))

    return True

  def startSession(self) -> bool:
    '''
    Inicia una sessió amb la base de dades Oracle.

    Retorna:
    --------
    bool
        True si la sessió s'ha iniciat correctament, False en cas contrari.
  '''
    self.open()
    return self.isStarted

  def showMessages(self) -> None:
    '''
    Mostra els missatges de sortida de la base de dades Oracle.

    Retorna:
    --------
    None
    '''
    statusVar = self.cursor.var(NUMBER)
    lineVar = self.cursor.var(STRING)
    while True:
      self.cursor.callproc("dbms_output.get_line", (lineVar, statusVar))
      if statusVar.getvalue() != 0:
        break
      print(lineVar.getvalue())

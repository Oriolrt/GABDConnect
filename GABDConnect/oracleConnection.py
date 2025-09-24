# -*- coding: utf-8 -*-
u"""
Created on Jun 12, 2018

@author: Oriol Ramos Terrades
@email: oriol.ramos@uab.cat

copyright: 2018, Oriol Ramos Terrades

Aquest script forma part del material didàctic de l'assignatura de Gestió i Administració de Bases de Dades (GABD) de la
Universitat Autònoma de Barcelona. La classe `oracleConnection` proporciona una implementació específica per a la gestió
de connexions a bases de dades Oracle, incloent la configuració i manteniment de les connexions. Aquesta eina és
essencial per a l'administració segura i eficient de bases de dades Oracle en entorns distribuïts.
"""

import logging

from oracledb import *

from .AbsConnection import AbsConnection


class oracleConnection(AbsConnection):
    """
    Classe per gestionar la connexió a una base de dades Oracle.

    Atributs:
    ----------
    _cursor : oracledb.Cursor
        Cursor per executar procediments i consultes.
    _serviceName : str
        Nom del servei de la base de dades.
    _dsn : str
        Data Source Name per a la connexió a la base de dades.
    """

    __slots__ = ['_cursor', '_serviceName', '_con_params']

    def __init__(self, **params):
        """
        Constructor per inicialitzar la connexió Oracle.

        Paràmetres:
        ---------
        **params: dict
            Paràmetres de connexió, incloent `serviceName` i `port`.
        """

        self._cursor = None
        self._serviceName = params.pop('serviceName', 'orcl')
        params['port'] = params.pop('port', 1521)

        AbsConnection.__init__(self, **params)
        self._dsn = f"{self.user}/{self.pwd}@localhost:{self._local_port}/{self._serviceName}"

        # mode = params.pop('mode', None)

        mode = SYSDBA if params.pop('mode', '').strip().lower() in ['sysdba', 'dba'] else None

        self._con_params = {'mode': mode} if mode is not None else dict()

    def cursor(self) -> DB_TYPE_CURSOR:
        """
        Retorna el cursor de la connexió Oracle.

        Retorna:
        --------
        oracledb.Cursor
            El cursor de la connexió.
        """
        try:
            self._cursor = self.conn.cursor()
            self._cursor.callproc("dbms_output.enable")

        except DatabaseError:
            logging.warning('Database connection already closed')
            self._cursor = None
        except AttributeError as e:
            print("Error: la connexió és None, no puc obrir cursor.")
            print("Detall:", e)
            self._cursor = None
        finally:
            return self._cursor

    def open(self, dsn: str = None, host: str = None, port: int = None, service_name: str = None, **con_params):
        """
          Connect to a oracle server given the connexion information saved on the cfg member variable.

          :return: bool
        """

        if not AbsConnection.open(self):
            t = self.server
            raise RuntimeError(f"Could not open the SSH tunnel {t}. Check the connection parameters and its status.")

        # Si no es passa dsn, el creem a partir de host/port/service_name o de self._dsn
        if dsn is None:
            if host and port and service_name:
                self.dsn = makedsn(host, port, service_name=service_name)

        else:
            self.dsn = dsn

        con_params_to_use = {**self._con_params, **con_params}

        try:
            self.conn = self.open_session(**con_params_to_use)

        except DatabaseError as e:
            # self.closeTunnel()
            self.is_open = False
            logging.error(
                f"Could not open the connection with dsn: {self._dsn}. Check the connection parameters and its status." +
                f" Tunnel: {self.server}")
            logging.error(f"Error: {e}")
        finally:
            self._context_mode = "session"
            return self

    def open_session(self, **con_params):
        conn = connect(self.dsn, **con_params)
        if conn is not None:
            self.conn = conn
            self._cursor = self.conn.cursor()
            self.is_open = True
        else:
            self.is_open = False
            t = self.server
            raise RuntimeError(f"Could not open the connection with dsn: {self._dsn}. Check the connection " + \
                               f"parameters and its status. Tunnel: {t}")

        return self.conn

    def close(self) -> None:
        """
        Tanca la connexió a la base de dades Oracle.

        Retorna:
        --------
        None
        """
        try:
            self.conn.close()
            self.is_open = False
        except DatabaseError:
            logging.warning('Database connection already closed')
        except AttributeError:
            print(f"Connexió a {self._dsn} tancada.")

    def close_session(self) -> None:
        """
        Tanca la sessió actual a la base de dades Oracle i manté els tunels SSH oberts.

        Retorna:
        --------
        None
        """

        try:
            if self._cursor is not None:
                self._cursor.close()
        except Exception:
            self._cursor = None

        try:
            if self.conn is not None:
                self.conn.close()
        except DatabaseError:
            logging.warning('Database connection already closed')
        except AttributeError as e:
            print(f"Connexió a {self._dsn} tancada.")

        self.is_open = False

    def commit(self) -> None:
        """
        Fa un commit de la transacció actual.

        Retorna:
        --------
        None
        """
        self.conn.commit()

    def test_connection(self) -> bool:
        """
        Prova la connexió a la base de dades Oracle.

        Retorna:
        --------
        bool
            True si la connexió és correcta, False en cas contrari.
        """
        cur = self._cursor

        try:
            res = cur.execute("""SELECT sys_context('USERENV','SESSION_USER')  as "CURRENT USER" ,
                      sys_context('USERENV', 'CURRENT_SCHEMA') as "CURRENT SCHEMA"
                      FROM dual""").fetchone()

            print("Current user: {}, Current schema: {}".format(res[0], res[1]))
            return True
        except Exception:
            logging.error("Database is not open. Check the connection parameters and its status.")
            return False

    def startSession(self) -> bool:
        """
        Inicia una sessió amb la base de dades Oracle.

        Retorna:
        --------
        bool
            True si la sessió s'ha iniciat correctament, False en cas contrari.
        """
        self.open()
        return self.is_open

    def showMessages(self) -> None:
        """
        Mostra els missatges de sortida de la base de dades Oracle.

        Retorna:
        --------
        None
        """
        statusVar = self._cursor.var(NUMBER)
        lineVar = self._cursor.var(STRING)
        while True:
            self._cursor.callproc("dbms_output.get_line", (lineVar, statusVar))
            if statusVar.getvalue() != 0:
                break
            print(lineVar.getvalue())

    @property
    def is_open(self) -> bool:
        """
        Retorna True si la connexió Oracle està oberta.
        """
        if not hasattr(self, "conn") or self.conn is None:
            return False

        try:
            return self.conn.ping() is None  # retorna None si la connexió és vàlida
        except Exception:
            return False

    @is_open.setter
    def is_open(self, valor: bool) -> None:
        # Setter buit per evitar errors en assignacions
        self._is_open = valor



    # Context manager
    def __enter__(self):

        success = self.open()  # sense arguments → utilitza self._dsn
        if not success:
            raise RuntimeError("No s'ha pogut obrir la connexió Oracle")
        # marquem que el context és a nivell de túnel
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
        #return False  # no suprimim excepcions

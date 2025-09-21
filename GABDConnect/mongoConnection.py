# -*- coding: utf-8 -*-
u"""
Created on Jun 12, 2018

@author: Oriol Ramos Terrades
@email: oriol.ramos@uab.cat

copyrigth: 2018, Oriol Ramos Terrades

Aquest script és part del material didàctic de l'assignatura de Gestió i Administració de Bases de Dades (GABD) de la Universitat Autònoma de Barcelona. La classe `mongoConnection` és una eina poderosa dissenyada per facilitar la connexió i gestió de bases de dades MongoDB. Amb aquesta classe, els estudiants aprendran a establir connexions segures, gestionar sessions i interactuar amb bases de dades NoSQL, habilitats essencials per a l'administració moderna de bases de dades en entorns distribuïts i escalables.
"""

from pymongo import MongoClient, errors
from pymongo.errors import ServerSelectionTimeoutError

from .AbsConnection import AbsConnection


class mongoConnection(AbsConnection):
    """
      Classe per gestionar la connexió a una base de dades MongoDB.
    """

    __slots__ = ['_auth_db', '_bd', '_auth_activated','_bd_name','_mongo_uri']

    def __init__(self, **params):
        '''
        Constructor per inicialitzar la connexió MongoDB amb els paràmetres donats.

        Paràmetres:
        -----------
        **params : dict
            Paràmetres de connexió, incloent `auth_db`, `auth_activated`, `db_name`, `user`, `passwd`, `hostname` i `port`.
        '''

        self._auth_db = params.pop('auth_db', "admin")
        self._auth_activated = params.pop('auth_activated', False)
        self._bd_name = params.pop('db_name', 'test')
        self._bd = None
        params['user'] = params.pop('user', None)
        params['hostname'], params['port'] = params.pop('hostname', 'localhost'), params.pop('port', 27017)

        AbsConnection.__init__(self,**params)

        self._auth_activated = self.user is not None and (isinstance(self.user,str) and len(self.user) > 0)

        if not self._auth_activated:
            self._mongo_uri = f"mongodb://localhost:{self._local_port}/{self._auth_db}"
        else:
            self._mongo_uri = f"mongodb://{params['user']}:{params['pwd']}@localhost:{self._local_port}/{self._auth_db}"

    @property
    def bd(self):
        if not self.is_started:
            self.startSession()

        return super(mongoConnection, self).bd

    @bd.setter
    def bd(self, db):
        self._bd = db


    @property
    def bd_name(self):
        return self._bd_name

    @bd_name.setter
    def bd_name(self, value):
        self._bd_name = value

    def open(self):
        """
        Connecta a un servidor MongoDB amb la informació de connexió guardada.

        Retorna:
        --------
        None
        """

        AbsConnection.open(self)

        try:
            self.conn = MongoClient(self._mongo_uri, serverSelectionTimeoutMS=100)
            self.conn.server_info()  # force connection on a request as the  # connect=True parameter of MongoClient seems  # to be useless here
            self.is_started = True
            self.bd = self.conn[self.bd_name]
            print("Connexió a MongoDB oberta.")
        except ServerSelectionTimeoutError as err:
            self.closetunnel()
            self.is_started = False
            # do whatever you need
            print(err)

        return self.conn

    def close_session(self):
        """
        Tanca la sessió amb la base de dades MongoDB.

        Retorna:
        --------
        None
        """
        try:
            if self.conn:
                self.conn.close()
                print("[INFO] Sessió MongoDB tancada correctament.")
            else:
                print("[WARN] No hi havia connexió MongoDB activa.")

            self.is_started = False

        except errors.PyMongoError as e:
            print(f"[ERROR] Error en tancar la sessió MongoDB: {e}")
        except AttributeError:
            print("[WARN] L'objecte MongoClient no existeix (self.conn és None).")

        finally:
            self.conn = None
            self.bd = None
            self.is_started = False

    def close(self):
        """
        Tanca la connexió a la base de dades MongoDB i el túnel SSH associat.

        Retorna:
        --------
        None
        """
        try:
            if self.conn:
                self.conn.close()
                print("[INFO] Connexió MongoDB tancada correctament.")
            else:
                print("[WARN] No hi havia connexió MongoDB activa.")

            # Tancar el forward/túnel associat
            self.closetunnel()

        except errors.PyMongoError as e:
            print(f"[ERROR] Error en tancar la connexió MongoDB: {e}")
        except AttributeError:
            print("[WARN] L'objecte MongoClient no existeix (self.conn és None).")

        finally:
            self.conn = None
            self.bd = None
            self.is_started = False

    def startSession(self):
        """
        Inicia una sessió amb la base de dades MongoDB.

        Retorna:
        --------
        bool
            True si la sessió s'ha iniciat correctament, False en cas contrari.
        """
        self.open()
        return self.is_started

    def test_connection(self):
        """
        Prova la connexió a la base de dades MongoDB.

        Retorna:
        --------
        bool
            True si la connexió és correcta, False en cas contrari.
        """
        try:
            dbs = self.conn.list_database_names()
            print("Databases: {}".format(" ".join(dbs)))
        except ServerSelectionTimeoutError as err:
            print(f"error:\n {err}")
            return False

        return True

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

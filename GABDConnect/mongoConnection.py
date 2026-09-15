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
import logging
# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class mongoConnection(AbsConnection):
    """
      Classe per gestionar la connexió a una base de dades MongoDB.
    """

    __slots__ = ['_auth_db', '_bd', '_auth_activated','_bd_name']

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

        # Igual que oracleConnection: si no hi ha ssh_data, cal connectar directament a
        # self.hostname:self.port -- abans es feia servir sempre "localhost:{_local_port}",
        # ignorant l'hostname real quan no hi havia túnel SSH (connexions Mongo directes
        # sempre acabaven intentant connectar a localhost). A més, `params['pwd']` no
        # existia mai com a clau (AbsConnection.__init__ l'ha consumit com 'passwd', no
        # 'pwd'), així que la branca amb autenticació sempre petava amb KeyError abans
        # d'arribar-hi -- cal self.pwd (la property), no params['pwd'].
        if not params.get('ssh_data'):
            target = f"{self.hostname}:{self.port}"
        else:
            target = f"localhost:{self._local_port}"

        if not self._auth_activated:
            self.dsn = f"mongodb://{target}/{self._auth_db}"
        else:
            self.dsn = f"mongodb://{params['user']}:{self.pwd}@{target}/{self._auth_db}"

    @property
    def bd(self):
        # No existeix cap propietat `is_started` (ni `super().bd`, ja que AbsConnection
        # no en defineix cap) -- accedir a .bd sempre petava amb AttributeError. `_bd`
        # és l'atribut real que emplena open_session() (self.bd = self.conn[self.bd_name]).
        if not self.is_open:
            self.startSession()

        return self._bd

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
            if self.is_open:
                # Tanquem la connexió anterior si ja estava oberta
                self.close_session()

            self.open_session()
            logger.debug("Connexió a MongoDB oberta.")
        except ServerSelectionTimeoutError as err:
            self.closetunnel()
            # self.is_started = False
            self.conn = None
            logger.error(f"[ERROR] No s'ha pogut connectar a MongoDB: {err}")
            # do whatever you need
        except errors.OperationFailure as e:
            self.closetunnel()
            # self.is_started = False
            self.conn = None
            raise


        return self.conn

    def open_session(self):
        self.conn = MongoClient(self.dsn, serverSelectionTimeoutMS=100)
        self.conn.server_info()  # force connection on a request as the  # connect=True parameter of MongoClient seems  # to be useless here
        # self.is_started = True
        self.bd = self.conn[self.bd_name]

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
                logger.info("[INFO] Sessió MongoDB tancada correctament.")
            else:
                logger.warning("[WARN] No hi havia connexió MongoDB activa.")

            #self.is_started = False

        except errors.PyMongoError as e:
            logger.error(f"[ERROR] Error en tancar la sessió MongoDB: {e}")
        except AttributeError:
            logger.error("[WARN] L'objecte MongoClient no existeix (self.conn és None).")

        finally:
            self.conn = None
            self.bd = None
            # self.is_started = False

    def close(self):
        """
        Tanca la connexió a la base de dades MongoDB i el túnel SSH associat.

        Retorna:
        --------
        None
        """
        try:
            if self.conn:
                self.close_session()
                print("[INFO] Connexió MongoDB tancada correctament.")
            else:
                print("[WARN] No hi havia connexió MongoDB activa.")

            # Tancar el forward/túnel associat
            self.closetunnel()

        except errors.PyMongoError as e:
            print(f"[ERROR] Error en tancar la connexió MongoDB: {e}")
        except AttributeError:
            print("[WARN] L'objecte MongoClient no existeix (self.conn és None).")

        # finally:
        #    self.conn = None
        #    self.bd = None
        #    self.is_started = False

    def startSession(self):
        """
        Inicia una sessió amb la base de dades MongoDB.

        Retorna:
        --------
        bool
            True si la sessió s'ha iniciat correctament, False en cas contrari.
        """
        self.open()
        return self.is_open

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



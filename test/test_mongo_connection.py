import unittest
from GABDConnect.mongoConnection import mongoConnection
from GABDConnect.ssh_tunnel import get_free_port
import os
from pymongo.errors import OperationFailure, PyMongoError
import socks
import socket
import sys
import traceback
import time
import subprocess


USED_PORTS = {int(1521)}


def get_unique_free_port():
    port = get_free_port()
    while port in USED_PORTS:
        port = get_free_port()
    USED_PORTS.add(port)
    return port


class MongoConnectTestCase(unittest.TestCase):
    def setUp(self):
        ssh_host = os.environ.get("SSH_HOST")
        ssh_user = os.environ.get("SSH_USER")
        ssh_port = int(os.environ.get("SSH_PORT", 8192))
        ssh_pwd = os.environ.get("SSH_PWD")
        ssh_key_local = "../dev_keys/id_student"
        ssh_key_home = os.path.expanduser("~/.ssh/id_student")

        ssh_key_path = (
            ssh_key_local if os.path.exists(ssh_key_local)
            else ssh_key_home if os.path.exists(ssh_key_home)
            else "ssh_key"
        )

        missing_host_or_user = not ssh_host or not ssh_user
        missing_auth = not (os.path.exists(ssh_key_path) or ssh_pwd)

        if missing_host_or_user or missing_auth:
            self.skipTest("SSH credentials not provided in environment variables or key file not found")


        self.ssh_server = {
            'ssh': ssh_host,
            'user': ssh_user,
            'port': ssh_port,
        }

        if ssh_pwd:
            self.ssh_server['pwd'] = ssh_pwd

        if os.path.exists(ssh_key_path):
            self.ssh_server['id_key'] = ssh_key_path


        self.hostname = "localhost"
        self.port = 27017
        self.db = "test"

    def tearDown(self):
        # Aquí alliberes túnels després de cada test
        mongoConnection.close_all_tunnels()

    def test_mongoDB_default_connection(self):
        with mongoConnection(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, db=self.db) as client:
            self.assertTrue(client.test_connection(), "La connexió ha fallat")
            self.assertIsNotNone(client.conn, "MongoDB client should be initialized")
            # Comprovem que la connexió es tanca correctament
            tunnel = client.get_tunnel()

        # Comprovem que la connexió es tanca correctament
        self.assertTrue(tunnel.is_tunnel_closed(), "MongoDB client should be closed")

    def test_mongoDB_local_port_connection(self):
        self.local_port = get_unique_free_port()
        with  mongoConnection(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, db=self.db,
                              local_port=self.local_port) as client:
            self.assertTrue(client.test_connection(), "La connexió ha fallat")
            self.assertIsNotNone(client.conn, "MongoDB client should be initialized")
            # Comprovem que la connexió es tanca correctament
            tunnel = client.get_tunnel()

        # Comprovem que la connexió es tanca correctament
        self.assertTrue(tunnel.is_tunnel_closed(), "MongoDB client should be closed")

    def test_mongoDB_tunnel_local_connection(self):
        self.hostname = "main.grup00.gabd"
        with mongoConnection(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, db=self.db) as client:
            self.assertTrue(client.test_connection(), "La connexió ha fallat")
            db = client.conn[client.bd_name]
            self.assertIsNotNone(db,
                                 f"Should be able to connect to the MongoDB database in {self.hostname} through SSH tunnel")
            # Comprovem que la connexió es tanca correctament
            tunnel = client.get_tunnel()

        # Comprovem que la connexió es tanca correctament
        self.assertTrue(tunnel.is_tunnel_closed(), "MongoDB client should be closed")

    def test_mongoDB_crud_basic(self):
        bd_name = "test_mongo"
        col_name = "col_test"
        local_port = get_unique_free_port()
        data = [{"name": "Oriol", "surname": "Ramos"},
                {"name": "Pere", "surname": "Roca"},
                {"name": "Anna", "surname": "Roca"}]

        with mongoConnection(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server,
                             db=self.db, local_port=local_port) as client:

            db = client.conn[bd_name]
            # creeem una col·lecció si no existeix
            try:
                col = db[col_name]
            except:
                col = db.create_collection(col_name)

            # Insertem dades a la col·lecció
            col.insert_many(data)

            try:
                # Check if the data is inserted
                self.assertEqual(
                    3,
                    col.count_documents({}),
                    f"Should be able to insert data in the MongoDB database in {self.hostname} through SSH tunnel"
                )
            except AssertionError as e:
                # Cleanup si el test falla
                db.drop_collection(col_name)
                raise e  # torna a llençar l'error perquè el test marqui com a fallit

            # Fem una cerca ala col·lecció
            for doc in col.find():
                print(doc)

            # Eliminem un document de la col·lecció
            col.delete_one({"name": "Anna"})

            # Eliminem la col·lecció
            db.drop_collection(col_name)

            tunnel = client.get_tunnel()

        # Comprovem que la connexió es tanca correctament
        self.assertTrue(tunnel.is_tunnel_closed(local_port), "MongoDB client should be closed")

    def test_user_data_connection_without_authentication(self):
        self.hostname = "mongo-1.grup00.gabd"
        self.local_port = get_unique_free_port()
        self.user = ""
        self.pwd = ""
        with mongoConnection(user=self.user, pwd=self.pwd, hostname=self.hostname, port=self.port,
                             ssh_data=self.ssh_server,
                             db=self.db) as client:
            db = client.conn[client.bd_name]
            self.assertIsNotNone(db,
                                 f"Should be able to connect to the MongoDB database in {self.hostname} through SSH tunnel")
            tunnel = client.get_tunnel()

        # Comprovem que la connexió es tanca correctament
        self.assertTrue(tunnel.is_tunnel_closed(self.local_port), "MongoDB client should be closed")

    def test_user_data_connection_with_authentication(self):
        self.hostname = "main.grup00.gabd"
        self.local_port = get_unique_free_port()
        self.user = "gestorGeonames"
        self.pwd = "gGeonames_pwd"
        self.bd = "Practica_3"

        try:
            # Intentem amb autenticació
            with mongoConnection(
                user=self.user,
                pwd=self.pwd,
                hostname=self.hostname,
                port=self.port,
                ssh_data=self.ssh_server,
                db=self.bd
            ) as client:
                db = client.conn[client.bd_name]
                col = db["geonames"]
                for doc in col.find().limit(10):
                    print(doc)

                tunnel = client.get_tunnel()

            # Comprovem que la connexió es tanca correctament
            self.assertTrue(tunnel.is_tunnel_closed(self.local_port), "MongoDB client should be closed")

        except OperationFailure:
            # Si Mongo no té auth activada → reintent sense credencials
            print("[WARN] Autenticació fallida o no activada...")
        except Exception as e:
            self.fail(f"Unexpected exception occurred: {e}")

    def test_without_ssh_tunnel(self):
        """
        Obrir connexió directa sense necessitat de fer un tunnel ssh.

        """

        port=1080
        self.hostname = "main.grup00.gabd"
        self.local_port = get_unique_free_port()
        self.user = "gestorGeonames"
        self.pwd = "grup00"
        self.db_name = "Practica_3"
        self.port = 27017
        #self.db = "test"


        cmd = [
            "ssh", "-T",
            "-p", str(self.ssh_server['port']),
            "-i", self.ssh_server['id_key'],
            "-D", f"{port}",
            f"{self.ssh_server['user']}@{self.ssh_server['ssh']}"
        ]

        # Obre el túnel en segon pla
        proc = subprocess.Popen(cmd)

        print("Túnel creat (PID {})".format(proc.pid))

        # Espera uns segons perquè estigui actiu
        time.sleep(2)

        # --- 1) Configura SOCKS amb resolució remota ---
        socks.set_default_proxy(socks.SOCKS5, "localhost", port, rdns=True)

        # --- 2) Monkeypatch del socket per redirigir tot via SOCKS ---
        original_socket_class = socket.socket
        socket.socket = socks.socksocket

        # --- 3) Monkeypatch de getaddrinfo per evitar resolució local ---
        original_getaddrinfo = socket.getaddrinfo

        def _fake_getaddrinfo(host, port, family=0, type=0, proto=0, flags=0):
            # Retorna una entrada mínima que deixa passar el hostname sense resoldre
            return [(socket.AF_INET, socket.SOCK_STREAM, proto, "", (host, port))]

        socket.getaddrinfo = _fake_getaddrinfo

        # --- 4) Timeout generós ---
        socket.setdefaulttimeout(30)

        connexio = None
        try:
            # Validació ràpida: TCP via SOCKS (ha d'anar bé)
            s = socket.socket()
            s.connect((self.hostname, self.port))
            print(f"TCP via SOCKS OK a {self.hostname}:{self.port}")
            s.close()


            # Aquí pots fer la connexió Oracle amb cx_Oracle o SQLAlchemy
            print(f"Ara pots connectar-te a MongoDB via {self.hostname}:{self.port}")
            # Crear client Oracle amb túnel SSH
            with mongoConnection(
                user=self.user,
                pwd=self.pwd,
                hostname=self.hostname,
                port=self.port,
                ssh_data=None, # No cal túnel SSH ja que ja estem fent SOCKS
                local_port=None,  # No local port needed
                db_name=self.db_name
            ) as client:
                db = client.conn[client.bd_name]
                col = db["geonames"]
                for doc in col.find().limit(10):
                    print(doc)


            time.sleep(5)
            # Quan vulguis tancar el túnel:
            proc.terminate()
        except PyMongoError as e:
            print("Error de connexió MongoDB:", e)
            traceback.print_exc(file=sys.stdout)
            self.AssertFail("No s'ha pogut connectar a MongoDB via túnel SSH")

        except Exception as e:
            print(f"No s'ha pogut establir la connexió: {e}")
            traceback.print_exc(file=sys.stdout)
            self.fail("No s'ha pogut connectar a MongoDB via túnel SSH")

        finally:
            # --- 5) Restaura els monkeypatches i tanca ---
            socket.getaddrinfo = original_getaddrinfo
            socket.socket = original_socket_class





if __name__ == '__main__':
    unittest.main()

import unittest
from GABDConnect.mongoConnection import mongoConnection
from GABDConnect.ssh_tunnel import get_free_port
import os
from pymongo.errors import OperationFailure
import subprocess
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
        Obrir connexió directa sense necessitat de fer un tunnel ssh

        """

        local_port = get_unique_free_port()
        hostname='localhost'
        port=local_port

        cmd = [
            "ssh", "-T",
            "-p", str(self.ssh_server['port']),
            "-i", self.ssh_server['id_key'],
            "-L", f"{local_port}:{self.hostname}:{self.port}",
            f"{self.ssh_server['user']}@{self.ssh_server['ssh']}"
        ]

        # Obre el túnel en segon pla
        proc = subprocess.Popen(cmd)

        print("Túnel creat (PID {})".format(proc.pid))

        # Espera uns segons perquè estigui actiu
        time.sleep(2)


        # Aquí pots fer la connexió Oracle amb cx_Oracle o SQLAlchemy
        print(f"Ara pots connectar-te a MongoDB via {hostname}:{local_port}")

        bd_name = "test_mongo"
        col_name = "col_test"

        data = [{"name": "Oriol", "surname": "Ramos"},
                {"name": "Pere", "surname": "Roca"},
                {"name": "Anna", "surname": "Roca"}]

        with mongoConnection(hostname=hostname, port=local_port,
                             db=self.db) as client:

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

        # Mantén el túnel obert durant un temps per a proves
        time.sleep(5)
        # Quan vulguis tancar el túnel:
        proc.terminate()

        # Comprovem que la connexió es tanca correctament
        self.assertIsNone(tunnel, "Connexió hauria d'estar tancada")




if __name__ == '__main__':
    unittest.main()

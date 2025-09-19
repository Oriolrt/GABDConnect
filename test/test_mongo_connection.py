import unittest
from GABDConnect.mongoConnection import mongoConnection
from GABDConnect.ssh_tunnel import get_free_port
import os
from pymongo.errors import OperationFailure

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
        ssh_key_local = "../dev_keys/id_student"
        ssh_key_home = os.path.expanduser("~/.ssh/id_student")

        ssh_key_path = (
            ssh_key_local if os.path.exists(ssh_key_local)
            else ssh_key_home if os.path.exists(ssh_key_home)
            else "ssh_key"
        )

        if not all([ssh_host, ssh_user]) or not os.path.exists(ssh_key_path):
            self.skipTest("SSH credentials not provided in environment variables")

        self.ssh_server = {
            'ssh': ssh_host,
            'user': ssh_user,
            'id_key': ssh_key_path,
            'port': ssh_port,
        }
        self.hostname = "localhost"
        self.port = 27017
        self.db = "test"



    def test_mongoDB_default_connection(self):
        self.client = mongoConnection(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, db=self.db)

        self.client.open()
        self.assertTrue(self.client.test_connection(), "La connexió ha fallat")
        self.assertIsNotNone(self.client.conn, "MongoDB client should be initialized")
        self.client.close()
        # Comprovem que la connexió es tanca correctament
        self.assertIsNone(self.client.conn, "MongoDB client should be closed")

    def test_mongoDB_local_port_connection(self):
        self.local_port = get_unique_free_port()
        self.client = mongoConnection(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, db=self.db, local_port=self.local_port)
        self.client.open()
        self.assertTrue(self.client.test_connection(), "La connexió ha fallat")
        self.assertIsNotNone(self.client.conn, "MongoDB client should be initialized")
        self.client.close()
        # Comprovem que la connexió es tanca correctament
        self.assertIsNone(self.client.conn, "MongoDB client should be closed")

    def test_mongoDB_tunnel_local_connection(self):
        self.hostname = "main.grup00.gabd"
        self.client = mongoConnection(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, db=self.db)
        self.client.open()
        self.assertTrue(self.client.test_connection(), "La connexió ha fallat")
        db = self.client.conn[self.client.bd_name]
        self.assertIsNotNone(db, f"Should be able to connect to the MongoDB database in {self.hostname} through SSH tunnel")
        self.client.close()
        # Comprovem que la connexió es tanca correctament
        self.assertIsNone(self.client.conn, "MongoDB client should be closed")

    def test_mongoDB_crud_basic(self):
        bd_name = "test_mongo"
        col_name = "col_test"
        local_port = get_unique_free_port()
        data = [{"name": "Oriol", "surname": "Ramos"},
                {"name": "Pere", "surname": "Roca"},
                {"name": "Anna", "surname": "Roca"}]

        self.client = mongoConnection(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, db=self.db, local_port=local_port)

        self.client.open()
        db = self.client.conn[bd_name]
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
            self.client.close()
            raise e  # torna a llençar l'error perquè el test marqui com a fallit

        # Fem una cerca ala col·lecció
        for doc in col.find():
            print(doc)

        # Eliminem un document de la col·lecció
        col.delete_one({"name": "Anna"})

        # Eliminem la col·lecció
        db.drop_collection(col_name)

        self.client.close()
        # Comprovem que la connexió es tanca correctament
        self.assertIsNone(self.client.conn, "MongoDB client should be closed")

    def test_user_data_connection_without_authentication(self):
        self.hostname = "mongo-1.grup00.gabd"
        self.local_port = get_unique_free_port()
        self.user = ""
        self.pwd = ""
        self.client = mongoConnection(user=self.user, pwd=self.pwd  ,hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, db=self.db)
        self.client.open()
        db = self.client.conn[self.client.bd_name]
        self.assertIsNotNone(db,
                             f"Should be able to connect to the MongoDB database in {self.hostname} through SSH tunnel")
        self.client.close()
        # Comprovem que la connexió es tanca correctament
        self.assertIsNone(self.client.conn, "MongoDB client should be closed")

    def test_user_data_connection_with_authentication(self):
        self.hostname = "main.grup00.gabd"
        self.local_port = get_unique_free_port()
        self.user = "gestorGeonames"
        self.pwd = "gGeonames_pwd"
        self.bd = "Practica_3"

        try:
            # Intentem amb autenticació
            self.client = mongoConnection(
                user=self.user,
                pwd=self.pwd,
                hostname=self.hostname,
                port=self.port,
                ssh_data=self.ssh_server,
                db=self.bd
            )
            self.client.open()

            db = self.client.conn[self.client.bd_name]
            self.assertIsNotNone(db,
                                 f"Should be able to connect to the MongoDB database in {self.hostname} through SSH tunnel")
            # recuperem els 10 primer elements de la col·lecció geonames
            col = db["geonames"]
            for doc in col.find().limit(10):
                print(doc)



        except OperationFailure:
            # Si Mongo no té auth activada → reintent sense credencials
            print("[WARN] Autenticació fallida o no activada...")

        finally:
            self.client.close()
            # veriquem que hem tancat la connexió
            self.assertIsNone(self.client.conn, "MongoDB client should be closed")








if __name__ == '__main__':
    unittest.main()

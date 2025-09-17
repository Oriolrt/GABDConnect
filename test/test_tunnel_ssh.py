import unittest
from GABDConnect import GABDSSHTunnel
import os


class GABDSSHTunnelTestCase(unittest.TestCase):
    def setUp(self):
        # Llegir config de secrets/entorn
        ssh_host = os.environ.get("SSH_HOST")
        ssh_user = os.environ.get("SSH_USER")
        ssh_pwd = os.environ.get("SSH_PWD")
        ssh_key_path = "../dev_keys/id_student" if os.path.exists("../dev_keys/id_student") else "ssh_key"
        ssh_port = int(os.environ.get("SSH_PORT", 22))

        if not all([ssh_host, ssh_user, ssh_pwd]):
            self.skipTest("SSH credentials not provided in environment variables")

        self.hostname = "localhost"
        self.local_port = 2222
        self.port = 22
        self.ssh_server = {
            'ssh': ssh_host,
            'user': ssh_user,
            'pwd': ssh_pwd,
            'port': ssh_port,
        }
        self.multiple_tunnels = {
            1521: "oracle-1.grup00.gabd:1521",
            1522: ("oracle-2.grup00.gabd", 1521),
            2222: ("oracle-2.grup00.gabd", 22)
        }

    def test_ssh_tunnel_connection(self):
        server = GABDSSHTunnel(hostname=self.hostname, port=self.port,
                               ssh_data=self.ssh_server, local_port=self.local_port)
        server.openTunnel()
        self.assertIsNotNone(server)
        server.closeTunnel()

    def test_ssh_tunnel_connection_oracle_1(self):
        hostname = "oracle-1.grup00.gabd"
        local_port = 1521
        server = GABDSSHTunnel(hostname=hostname, port=self.port,
                               ssh_data=self.ssh_server, local_port=local_port,
                               multiple_tunnels=self.multiple_tunnels)
        server.openTunnel()
        self.assertIsNotNone(server)
        server.closeTunnel()

    def test_ssh_tunnel_connection_oracle_2(self):
        hostname = "oracle-1.grup00.gabd"
        local_port = 1522
        server = GABDSSHTunnel(hostname=hostname, port=self.port,
                               ssh_data=self.ssh_server, local_port=local_port,
                               multiple_tunnels=self.multiple_tunnels)
        server.openTunnel()
        self.assertIsNotNone(server)
        server.closeTunnel()


if __name__ == '__main__':
    unittest.main()

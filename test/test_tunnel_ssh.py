import unittest
from GABDConnect import GABDSSHTunnel
from GABDConnect.ssh_tunnel import get_free_port
import os

USED_PORTS = set()

def get_unique_free_port(port = None):
    port = port if port is not None else get_free_port()
    while port in USED_PORTS:
        port = get_free_port()
    USED_PORTS.add(port)
    return port

class GABDSSHTunnelTestCase(unittest.TestCase):
    def setUp(self):
        # Llegir config de secrets/entorn
        ssh_host = os.environ.get("SSH_HOST")
        ssh_user = os.environ.get("SSH_USER")
        ssh_pwd = os.environ.get("SSH_PWD")
        # ssh_key_path = "../dev_keys/id_student" if os.path.exists("../dev_keys/id_student") else "ssh_key"
        ssh_port = int(os.environ.get("SSH_PORT", 22))

        if not all([ssh_host, ssh_user, ssh_pwd]):
            self.skipTest("SSH credentials not provided in environment variables")

        self.hostname = "localhost"
        self.local_port = get_unique_free_port(2222)
        self.port = 22
        self.ssh_server = {
            'ssh': ssh_host,
            'user': ssh_user,
            'pwd': ssh_pwd,
            'port': ssh_port,
        }
        self.multiple_tunnels = {
            get_unique_free_port(1521): ("oracle-1.grup00.gabd",1521),
            get_unique_free_port(1522): ("oracle-2.grup00.gabd", 1521),
            self.local_port: ("oracle-2.grup00.gabd", 22)
        }

    def test_ssh_tunnel_connection(self):
        """
        Test SSH tunnel connection to localhost
        """
        with  GABDSSHTunnel(hostname=self.hostname, port=self.port,
                               ssh_data=self.ssh_server) as server:
            self.assertTrue(server.is_active())

    def test_ssh_tunnel_connection_oracle_1(self):
        """
        Test SSH tunnel connection to oracle-1.grup00.gabd
        """
        hostname = "oracle-1.grup00.gabd"
        local_port = 1521
        with GABDSSHTunnel(hostname=hostname, port=self.port,
                               ssh_data=self.ssh_server, local_port=local_port,
                               multiple_tunnels=self.multiple_tunnels) as server:
            self.assertTrue(server.is_active())


    def test_ssh_tunnel_connection_oracle_2(self):
        """
        Test SSH tunnel connection to oracle-2.grup00.gabd
        """
        hostname = "oracle-1.grup00.gabd"
        local_port = 1522
        with GABDSSHTunnel(hostname=hostname, port=self.port,
                           ssh_data=self.ssh_server, local_port=local_port,
                           multiple_tunnels=self.multiple_tunnels) as server:

            self.assertTrue(server.is_active())

    def test_multiple_tunnels(self):
        """
        Test multiple SSH tunnels.
        1. Configura múltiples túneles SSH.
        2. Verifica que tots els túneles s'han establert correctament.
        3. Comprova que el nombre de túneles és correcte.
        4. Tanca els túneles i verifica que s'han tancat correctament.

        """
        tunnels = self.multiple_tunnels
        all_ssh_tunnels = set()

        for i,(h,rp,lp) in enumerate(zip([h[0] for h in list(tunnels.values())],
                           [h[1] for h in list(tunnels.values())],
                           [lp for lp in tunnels.keys()])):
            print(f"Local Port: {lp}, Host: {h},  Remote Port: {rp}")
            ssh_tunnel = GABDSSHTunnel(hostname=h, port=rp,
                           ssh_data=self.ssh_server, local_port=lp,
                           multiple_tunnels=self.multiple_tunnels)
            ssh_tunnel.opentunnel()
            # 2. Verifica que tots els túneles s'han establert correctament.
            self.assertTrue(ssh_tunnel.is_active())
            all_ssh_tunnels.add(ssh_tunnel)

        # 3. Comprova que el nombre de túneles és correcte.
        self.assertEqual(len(all_ssh_tunnels), len(tunnels))

        #4. Tanca els túneles i verifica que s'han tancat correctament.
        for t in all_ssh_tunnels:

            self.assertTrue(t.closetunnel())




if __name__ == '__main__':
    unittest.main()

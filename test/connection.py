import unittest
import getpass
from GABDConnect.oracleConnection import oracleConnection as orcl

class OracleConnectionTest(unittest.TestCase):
    def test_tunnel_defecte(self):
        ssh_tunnel = "dcccluster.uab.cat"
        ssh_user = "student"
        pwd = getpass.getpass("Enter password: ")
        port = getpass.getpass("Enter port: ")

        ssh_server = {'ssh': ssh_tunnel, 'user': ssh_user,
                      'pwd': pwd, 'port': port} if ssh_tunnel is not None else None

        # Dades de connexió a Oracle
        user = "ESPECTACLES"
        oracle_pwd = "ESPECTACLES"
        # port="1521"
        hostname = "oracle-1.grup00.gabd"
        # serviceName="orcl"

        db = orcl(user=user, passwd=oracle_pwd, hostname=hostname, ssh=ssh_server)

        db.open()

        self.assertEqual(True, False)  # add assertion here


if __name__ == '__main__':
    unittest.main()

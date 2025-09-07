import unittest
from GABDConnect import GABDSSHTunnel



class GABDSSHTunnelTestCase(unittest.TestCase):
  def setUp(self):
    self.hostname = "localhost"
    self.local_port = 2222
    self.port = 22
    self.ssh_server = {'ssh': "dcccluster.uab.cat", 'user': "student", 'id_key': "dev_keys/id_student", 'port': 8192}
    #self.ssh_server = {'ssh': "dcccluster.uab.cat", 'user': "student",  'port': 8192}
    self.multiple_tunnels = {1521: "oracle-1.grup00.gabd:1521", 1522: ("oracle-2.grup00.gabd", 1521),2222: ("oracle-2.grup00.gabd", 22)}


  def test_ssh_tunnel_connection(self):
    self.server = GABDSSHTunnel(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, local_port= self.local_port)
    self.server.openTunnel()
    self.assertIsNotNone(self.server, "Should be able to create a SSH tunnel")
    self.server.closeTunnel()

  def test_ssh_tunnel_connection_oracle_1(self):
    self.hostname = "oracle-1.grup00.gabd"
    self._local_port = 1522
    self.server = GABDSSHTunnel(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, local_port= self._local_port ,
                                multiple_tunnels=self.multiple_tunnels)
    self.server.openTunnel()
    self.assertIsNotNone(self.server, "Should be able to create a SSH tunnel")
    self.server.closeTunnel()

    def test_ssh_tunnel_connection_oracle_2(self):
      self.hostname = "oracle-2.grup00.gabd"
      self._local_port = 1522
      self.server = GABDSSHTunnel(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, local_port= self._local_port )
      self.server.openTunnel()
      self.assertIsNotNone(self.server, "Should be able to create a SSH tunnel")
      self.server.closeTunnel()


if __name__ == '__main__':
  unittest.main()

import unittest
from GABDConnect.oracleConnection import oracleConnection as orcl
import logging
import os


class OracleConnectTestCase(unittest.TestCase):
  def setUp(self):
    self.ssh_server = {'ssh': "dcccluster.uab.cat" , 'user': "student", 'id_key': "../dev_keys/id_student", 'port': 8192}
    #self.ssh_server = {'ssh': "dcccluster.uab.cat", 'user': "student", 'port': 8192}
    self.hostname = "oracle-1.grup00.gabd"
    self.port = 1521
    self.serviceName = "FREEPDB1"
    self.user = "ESPECTACLES"
    self.pwd = "ESPECTACLES"
    self.multiple_tunnels = {1521: "oracle-1.grup00.gabd:1521", 1522: ("oracle-2.grup00.gabd", 1521),
                             2222: ("oracle-2.grup00.gabd", 22)}

  def test_sshtunnel_default_connection(self):
    self.client = orcl(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, user=self.user,
                       passwd=self.pwd, serviceName=self.serviceName)
    self.client.open()
    self.assertIsNotNone(self.client, f"Should be able to connect to the Oracle database in {self.hostname} through SSH tunnel")

    self.client.close()
    self.assertEqual(False, self.client.isStarted, f"Database should be close and is {self.client.isStarted}")  # add assertion here

  def test_tunnel_shh_key(self):
    GRUP = "grup00"
    ssh_tunnel = self.ssh_server['ssh'] if self.ssh_server is not None else None
    SSH_USER = self.ssh_server['user'] if self.ssh_server is not None else None
    port = self.ssh_server['port'] if self.ssh_server is not None else None


    if os.path.isfile(f"../dev_keys/id_{SSH_USER}"):
      id_key = f"../dev_keys/id_{SSH_USER}"
      ssh_server = {'ssh': ssh_tunnel, 'user': SSH_USER,
                    'id_key': id_key, 'port': port} if ssh_tunnel is not None else None

    # Dades de connexió a Oracle
    user = "ESPECTACLES"
    oracle_pwd = "ESPECTACLES"
    hostname = f'oracle-1.{GRUP}.gabd'
    serviceName="FREEPDB1"

    # Cridem el constructor i obrim la connexió
    db = orcl(user=user, passwd=oracle_pwd, hostname=hostname,  ssh_data=ssh_server,serviceName=serviceName,
              multiple_tunnels=self.multiple_tunnels)
    db.open()

    if db.testConnection():
      logging.warning("La connexió a {} funciona correctament.".format(hostname))

    db.close()
    self.assertEqual(False, db.isStarted,
                     f"Database should be close and is {db.isStarted}")  # add assertion here
  def test_consulta_basica_connection(self):
    self._local_port = 1521
    self.client = orcl(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, user=self.user,
                       passwd=self.pwd, serviceName=self.serviceName,local_port=self._local_port)
    self.client.open()
    self.assertTrue(self.client.isStarted, f"Should be able to connect to the Oracle database in {self.hostname} through SSH tunnel")

    try:
      with self.client.cursor() as curs:
        curs.execute("""select 'Oriol' as nom, 'Ramos' as cognom 
        from dual 
        union
        select 'Carles' as nom, 'Sánchez' as cognom 
        from dual 
        """)
        for row in curs:
          print(row)
    except Exception as e:
      pass

    self.client.close()
    self.assertEqual(False, self.client.isStarted, f"Database should be close and is {self.client.isStarted}")  # add assertion here

  def test_dba_connection(self):
    self._local_port = 1521
    self.user= 'sys'
    self.pwd= 'oracle'
    self.mode='sysDBA'
    self.hostname = "oracle-2.grup00.gabd"

    self.client = orcl(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, user=self.user,
                       passwd=self.pwd, serviceName=self.serviceName,local_port=self._local_port,mode=self.mode)
    self.client.open()

    self.assertTrue(self.client.isStarted,
                         f"Should be able to connect to the Oracle database in {self.hostname} through SSH tunnel")

    try:
      with self.client.cursor() as curs:
        curs.execute("""select *
        from
        (SELECT i.instance_name,
         i.status AS instance_status,
         (SELECT d.open_mode FROM v$database d) AS database_open_mode,
         CASE 
           WHEN i.status = 'STARTED' THEN 'IDLE (només instància iniciada)'
           WHEN i.status = 'MOUNTED' THEN 'MUNTADA (BD muntada, no oberta)'
           WHEN i.status = 'OPEN' 
                AND (SELECT d.open_mode FROM v$database d) = 'READ WRITE'
                THEN 'OBERTA (lectura i escriptura)'
           WHEN i.status = 'OPEN' 
                AND (SELECT d.open_mode FROM v$database d) LIKE 'READ ONLY%'
                THEN 'OBERTA (només lectura)'
           ELSE 'ESTAT DESCONEGUT'
         END AS estat_complet
         FROM   v$instance i),
         (SELECT COUNT(*)              AS total_backups,
           MIN(b.completion_time) AS first_backup_date,
           MAX(b.completion_time) AS last_backup_date
          FROM   v$backup_piece p
           JOIN   v$backup_set b 
           ON p.set_stamp = b.set_stamp 
          AND p.set_count = b.set_count)
        """)
        for row in curs:
          print(row)
    except Exception as e:
      pass

    self.client.close()
    self.assertEqual(False, self.client.isStarted,
                     f"Database should be close and is {self.client.isStarted}")  # add assertion here

  def test_dba_multiple_connection(self):

    self.user= 'sys'
    self.pwd= 'oracle'
    self.mode='sysDBA'
    self.hostname = "oracle-2.grup00.gabd"
    self.multiple_tunnels = {1521: "oracle-1.grup00.gabd:1521", 1522: ("oracle-2.grup00.gabd", 1521)}

    group_tunnels = {}
    group_connections_info = {}
    line = f"grup00 {self.ssh_server['port']} {self.ssh_server['id_key']}"
    local_port_counter = 1521

    # Split the line by spaces or tabs
    parts = line.strip().split()
    if len(parts) == 3:
      group_name, PORT, ID_KEY = parts
      # Create the tunnels dictionary for the current group
      tunnels = {int(local_port_counter): f"oracle-1.{group_name}.gabd:1521",
                 int(local_port_counter + 1): (f"oracle-2.{group_name}.gabd", 1521)}
      # Store the tunnels dictionary in the group_tunnels dictionary
      group_tunnels[group_name] = tunnels
      #

      # Create connection information for the two Oracle servers
      conn_info_oracle1 = {
        'user': self.user,  # Assuming 'user' is defined in R9NyUeLD7ieV
        'passwd': self.pwd,  # Assuming 'oracle_pwd' is defined in R9NyUeLD7ieV
        'hostname': f"oracle-1.{group_name}.gabd",
        'ssh_data': {'ssh': self.ssh_server['ssh'], 'user': self.ssh_server['user'], 'id_key': ID_KEY, 'port': int(PORT)},
        # Updated ssh_data,
        'serviceName': self.serviceName,  # Assuming 'serviceName' is defined in R9NyUeLD7ieV
        'mode': self.mode,  # Assuming 'mode' is defined in R9NyUeLD7ieV
        'multiple_tunnels': self.multiple_tunnels.copy(),
        # 'local_port': local_port_counter # Add local_port key
      }
      local_port_counter += 1  # Increment local port counter

      conn_info_oracle2 = {
        'user': self.user,  # Assuming 'user' is defined in R9NyUeLD7ieV
        'passwd': self.pwd,  # Assuming 'oracle_pwd' is defined in R9NyUeLD7ieV
        'hostname': f"oracle-2.{group_name}.gabd",
        'ssh_data': {'ssh': self.ssh_server['ssh'], 'user': self.ssh_server['user'], 'id_key': ID_KEY, 'port': int(PORT)},
        # Updated ssh_data
        'serviceName': self.serviceName,  # Assuming 'serviceName' is defined in R9NyUeLD7ieV
        'mode': self.mode,  # Assuming 'mode' is defined in R9NyUeLD7ieV
        'multiple_tunnels': self.multiple_tunnels.copy(),
        # 'local_port': local_port_counter # Add local_port key
      }
      local_port_counter += 1  # Increment local port counter

      # Store the connection information in the group_connections_info dictionary
      group_connections_info[group_name] = [conn_info_oracle1, conn_info_oracle2]

    # Iterate through the group_connections_info dictionary and open connections
    all_dbs = []
    for group_name, connections_info in group_connections_info.items():
      print(f"\nCreating connections for group: {group_name}")
      # connections_info is a list of two dictionaries, one for each Oracle server in the group
      for conn_info in connections_info:
        # print conn_info
        print(conn_info)
        # Create the database connection object using the information in conn_info
        d = orcl(**conn_info)
        # Open connection
        if not d.isStarted:
          d.open()
        # Append the connection object to the all_dbs list
        all_dbs.append(d)

    self.client = orcl(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, user=self.user,
                       passwd=self.pwd, serviceName=self.serviceName,multiple_tunnels=self.multiple_tunnels,mode=self.mode)
    self.client.open()

    self.assertTrue(self.client.isStarted,
                         f"Should be able to connect to the Oracle database in {self.hostname} through SSH tunnel")

    for d in all_dbs:
      try:
        with d.cursor() as curs:
          curs.execute("""select *
          from
          (SELECT i.instance_name,
           i.status AS instance_status,
           (SELECT d.open_mode FROM v$database d) AS database_open_mode,
           CASE 
             WHEN i.status = 'STARTED' THEN 'IDLE (només instància iniciada)'
             WHEN i.status = 'MOUNTED' THEN 'MUNTADA (BD muntada, no oberta)'
             WHEN i.status = 'OPEN' 
                  AND (SELECT d.open_mode FROM v$database d) = 'READ WRITE'
                  THEN 'OBERTA (lectura i escriptura)'
             WHEN i.status = 'OPEN' 
                  AND (SELECT d.open_mode FROM v$database d) LIKE 'READ ONLY%'
                  THEN 'OBERTA (només lectura)'
             ELSE 'ESTAT DESCONEGUT'
           END AS estat_complet
           FROM   v$instance i),
           (SELECT COUNT(*)              AS total_backups,
             MIN(b.completion_time) AS first_backup_date,
             MAX(b.completion_time) AS last_backup_date
            FROM   v$backup_piece p
             JOIN   v$backup_set b 
             ON p.set_stamp = b.set_stamp 
            AND p.set_count = b.set_count)
          """)
          for row in curs:
            print(row)
      except Exception as e:
        pass

    for d in all_dbs:
       try:
          d.close()
          self.assertEqual(False, d.isStarted,
                           f"Database should be closed and is {d.isStarted}")  # add assertion here
       except Exception as e:
          print(f"Error closing connection: {e}")





if __name__ == '__main__':
  unittest.main()

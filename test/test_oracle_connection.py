import time
import unittest
from GABDConnect.oracleConnection import oracleConnection as orcl
from GABDConnect.ssh_tunnel import get_free_port
import logging
import os


class OracleConnectTestCase(unittest.TestCase):
    def setUp(self):
        # Comprovar fitxer local de credencials o usar el fitxer creat pel workflow
        ssh_key_local = "../dev_keys/id_student"
        ssh_key_home = os.path.expanduser("~/.ssh/id_student")

        ssh_key_path = (
            ssh_key_local if os.path.exists(ssh_key_local)
            else ssh_key_home if os.path.exists(ssh_key_home)
            else "ssh_key"
        )

        # Llegir credencials del workflow si no hi ha fitxer local
        ssh_host = os.environ.get("SSH_HOST", "dcccluster.uab.cat")
        ssh_user = os.environ.get("SSH_USER", "student")
        ssh_port = int(os.environ.get("SSH_PORT", 8192))

        self.ssh_server = {
            'ssh': ssh_host,
            'user': ssh_user,
            'id_key': ssh_key_path,
            'port': ssh_port
        }

        self.hostname = "oracle-1.grup00.gabd"
        self.port = 1521
        self.serviceName = "FREEPDB1"
        self.user = "ESPECTACLES"
        self.pwd = "ESPECTACLES"

        self.multiple_tunnels = {
            1521: "oracle-1.grup00.gabd:1521",
            1522: ("oracle-2.grup00.gabd", 1521),
            2222: ("oracle-2.grup00.gabd", 22)
        }

    def test_sshtunnel_default_connection(self):
        with orcl(hostname=self.hostname, port=self.port, ssh_data=self.ssh_server, user=self.user,
                           passwd=self.pwd, serviceName=self.serviceName) as client:
            client.open()
            self.assertIsNotNone(client, f"Should be able to connect to the Oracle database in {self.hostname} \
            through SSH tunnel")

            #client.close()
            #self.assertEqual(False, client.is_started, f"Database should be closed and is {client.is_started}")

        time.sleep(5)

    def test_tunnel_ssh_key(self):
        print("\nTest SSH tunnel with SSH key")
        GRUP = "grup00"

        ssh_tunnel = self.ssh_server['ssh'] if self.ssh_server else None
        SSH_USER = self.ssh_server['user'] if self.ssh_server else None
        port = self.ssh_server['port'] if self.ssh_server else None
        id_key = self.ssh_server['id_key'] if self.ssh_server else None

        # # Comprovar si existeix el fitxer local o utilitzar el fitxer creat pel workflow
        # if os.path.isfile(f"../dev_keys/id_{SSH_USER}"):
        #     id_key = f"../dev_keys/id_{SSH_USER}"
        # else:
        #     id_key = "ssh_key"  # fitxer creat pel workflow a partir del secret

        ssh_server = {
            'ssh': ssh_tunnel,
            'user': SSH_USER,
            'id_key': id_key,
            'port': port
        } if ssh_tunnel else None

        # Dades de connexió a Oracle
        user = "ESPECTACLES"
        oracle_pwd = "ESPECTACLES"
        hostname = f'oracle-1.{GRUP}.gabd'
        serviceName = "FREEPDB1"

        # Crear client Oracle amb túnel SSH
        with orcl(
            user=user,
            passwd=oracle_pwd,
            hostname=hostname,
            ssh_data=ssh_server,
            serviceName=serviceName,
            local_port=1524,
            multiple_tunnels=self.multiple_tunnels
        ) as db:

            try:
                # Obrir connexió
                db.open()
                self.assertTrue(
                    db.test_connection(),
                    f"Should be able to connect to Oracle database in {hostname} through SSH tunnel"
                )
                logging.warning(f"La connexió a {hostname} funciona correctament.")
            except Exception as e:
                self.fail(f"Failed to connect or test Oracle database: {e}")
            #finally:
            #    # Tancar connexió i comprovar estat
            #    db.close()
            #    self.assertFalse(
            #        db.is_started,
            #        f"Database should be closed and isStarted is {db.is_started}"
            #    )

        time.sleep(5)

    def test_consulta_basica_connection(self):
        print("\nTest: test_consulta_basica_connection")
        local_port = 1523  # get_free_port()

        # Crear client Oracle amb túnel SSH
        with orcl(
            hostname=self.hostname,
            port=self.port,
            ssh_data=self.ssh_server,
            user=self.user,
            passwd=self.pwd,
            serviceName=self.serviceName,
            local_port=local_port
        ) as client:

            # Obrir connexió
            client.open()
            self.assertTrue(
                client.is_started,
                f"Should be able to connect to the Oracle database in {self.hostname} through SSH tunnel"
            )

            try:
                # Executar consulta bàsica
                with client.cursor() as curs:
                    curs.execute("""
                    SELECT 'Oriol' AS nom, 'Ramos' AS cognom FROM dual
                    UNION
                    SELECT 'Carles' AS nom, 'Sánchez' AS cognom FROM dual
                """)
                    for row in curs:
                        print(row)
            except Exception as e:
                self.fail(f"Failed to execute basic query: {e}")
            #finally:
            #    # Tancar connexió
            #    client.close()
            #    self.assertFalse(
            #        client.is_started,
            #        f"Database should be closed and isStarted is {client.is_started}"
            #    )

        time.sleep(5)

    def test_dba_connection(self):
        print("\nTest DBA connection through SSH tunnel")
        # Configuració del test
        local_port = 1525
        user = 'sys'
        pwd = 'oracle'
        mode = 'sysDBA'
        hostname = "oracle-2.grup00.gabd"

        # Crear client Oracle amb túnel SSH
        with  orcl(
            hostname=hostname,
            port=self.port,
            ssh_data=self.ssh_server,
            user=user,
            passwd=pwd,
            serviceName=self.serviceName,
            local_port=local_port,
            mode=mode
        ) as client:

            # Obrir connexió
            client.open()
            self.assertTrue(
                client.is_started,
                f"Should be able to connect to the Oracle database in {hostname} through SSH tunnel"
            )

            try:
                # Executar consulta per obtenir estat de la base de dades i backups
                with client.cursor() as curs:
                    curs.execute("""
                    SELECT *
                    FROM
                    (SELECT i.instance_name,
                            i.status AS instance_status,
                            (SELECT d.open_mode FROM v$database d) AS database_open_mode,
                            CASE 
                                WHEN i.status = 'STARTED' THEN 'IDLE (només instància iniciada)'
                                WHEN i.status = 'MOUNTED' THEN 'MUNTADA (BD muntada, no oberta)'
                                WHEN i.status = 'OPEN' AND (SELECT d.open_mode FROM v$database d) = 'READ WRITE' THEN 'OBERTA (lectura i escriptura)'
                                WHEN i.status = 'OPEN' AND (SELECT d.open_mode FROM v$database d) LIKE 'READ ONLY%' THEN 'OBERTA (només lectura)'
                                ELSE 'ESTAT DESCONEGUT'
                            END AS estat_complet
                     FROM v$instance i),
                    (SELECT COUNT(*) AS total_backups,
                            MIN(b.completion_time) AS first_backup_date,
                            MAX(b.completion_time) AS last_backup_date
                     FROM v$backup_piece p
                     JOIN v$backup_set b 
                       ON p.set_stamp = b.set_stamp 
                      AND p.set_count = b.set_count)
                """)
                    for row in curs:
                        print(row)
            except Exception as e:
                self.fail(f"Failed to execute database query: {e}")
            #finally:
            #    # Tancar connexió
            #    client.close()
            #    self.assertFalse(
            #        client.is_started,
            #        f"Database should be closed and isStarted is {client.is_started}"
            #    )

        time.sleep(5)

    def test_dba_multiple_connection(self):
        print("\nTest multiple DBA connections through SSH tunnels")

        self.user = 'sys'
        self.pwd = 'oracle'
        self.mode = 'sysDBA'
        self.ssh_server['port'] = 8192
        self.hostname = None
        self.multiple_tunnels = None

        group_tunnels = {}
        group_connections_info = {}

        file = f"grup00 {self.ssh_server['port']} {self.ssh_server['id_key']}\n"
        file += f"grup01 {self.ssh_server['port']+1} {self.ssh_server['id_key']}"
        local_port_counter = 1530

        for line in file.strip().split('\n'):
            # Split the line by spaces or tabs
            parts = line.strip().split()
            if len(parts) == 3:
                group_name, PORT, ID_KEY = parts
                # Create the tunnels dictionary for the current group

                tunnels = {local_port_counter: f"oracle-1.{group_name}.gabd:1521",
                           local_port_counter+1: (f"oracle-2.{group_name}.gabd", 1521)}
                # Store the tunnels dictionary in the group_tunnels dictionary
                group_tunnels[group_name] = tunnels
                #

                # Create connection information for the two Oracle servers
                conn_info_oracle1 = {
                    'user': self.user,  # Assuming 'user' is defined in R9NyUeLD7ieV
                    'passwd': self.pwd,  # Assuming 'oracle_pwd' is defined in R9NyUeLD7ieV
                    'hostname': f"oracle-1.{group_name}.gabd",
                    'ssh_data': {'ssh': self.ssh_server['ssh'], 'user': self.ssh_server['user'], 'id_key': ID_KEY,
                                 'port': int(PORT)},
                    # Updated ssh_data,
                    'serviceName': self.serviceName,  # Assuming 'serviceName' is defined in R9NyUeLD7ieV
                    'mode': self.mode,  # Assuming 'mode' is defined in R9NyUeLD7ieV
                    'multiple_tunnels': tunnels.copy()
                    # 'local_port': local_port_counter # Add local_port key
                }
                # local_port_counter += 1  # Increment local port counter

                conn_info_oracle2 = {
                    'user': self.user,  # Assuming 'user' is defined in R9NyUeLD7ieV
                    'passwd': self.pwd,  # Assuming 'oracle_pwd' is defined in R9NyUeLD7ieV
                    'hostname': f"oracle-2.{group_name}.gabd",
                    'ssh_data': {'ssh': self.ssh_server['ssh'], 'user': self.ssh_server['user'], 'id_key': ID_KEY,
                                 'port': int(PORT)},
                    # Updated ssh_data
                    'serviceName': self.serviceName,  # Assuming 'serviceName' is defined in R9NyUeLD7ieV
                    'mode': self.mode,  # Assuming 'mode' is defined in R9NyUeLD7ieV
                    'multiple_tunnels': tunnels
                    # 'local_port': local_port_counter # Add local_port key
                }
                local_port_counter += 2  # Increment local port counter

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
                if not d.is_started:
                    d.open()
                # Append the connection object to the all_dbs list
                all_dbs.append(d)

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
            except Exception:
                pass

        for d in all_dbs:
            try:
                d.close()
                self.assertEqual(False, d.is_started,
                                 f"Database should be closed and is {d.is_started}")  # add assertion here
                del d
            except Exception as e:
                print(f"Error closing connection: {e}")

        time.sleep(5)


if __name__ == '__main__':
    unittest.main()

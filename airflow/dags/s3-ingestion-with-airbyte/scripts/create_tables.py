import pymysql
import sys
import os
import configparser

folder = os.path.dirname(os.path.abspath(__file__))
mysql_config_file = os.path.join(folder, "pipeline.conf")

parser = configparser.ConfigParser()
parser.read(mysql_config_file)


def connect_to_mysql(parser: configparser.ConfigParser):

    hostname = parser.get("mysql_config", "hostname")
    port = parser.get("mysql_config", "port")
    username = parser.get("mysql_config", "username")
    dbname = parser.get("mysql_config", "database")
    password = parser.get("mysql_config", "password")

    try:
        conn = pymysql.connect(
            host=hostname,
            user=username,
            passwd=password,
            db=dbname,
            connect_timeout=5,
            port=port,
        )

        return conn
    except pymysql.MySQLError as e:
        sys.exit()


def create_tables(conn: connect_to_mysql(parser)):

    with conn.cursor() as cur:
        cur.execute(
            """
         CREATE TABLE IF NOT EXISTS `customers` (
        `id` int NOT NULL AUTO_INCREMENT,
        `nome` text,
        `sexo` text,
        `endereco` text,
        `telefone` text,
        `email` text,
        `foto` text,
        `nascimento` date DEFAULT NULL,
        `profissao` text,
        `dt_update` timestamp NULL DEFAULT NULL,
        PRIMARY KEY (`id`)
        )
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS `credit_score` (
        `id` int NOT NULL AUTO_INCREMENT,
        `customer_id` int NOT NULL,
        `nome` text,
        `provedor` text,
        `credit_score` text,
        `dt_update` timestamp NULL DEFAULT NULL,
        PRIMARY KEY (`id`)
        )
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS `flight` (
        `id` int NOT NULL AUTO_INCREMENT,
        `customer_id` int NOT NULL,
        `aeroporto` text,
        `linha_aerea` text,
        `cod_iata` text,
        `dt_update` timestamp NULL DEFAULT NULL,
        PRIMARY KEY (`id`)
        )
        """
        )

        cur.execute(
            """
        CREATE TABLE IF NOT EXISTS `vehicle` (
        `id` int NOT NULL AUTO_INCREMENT,
        `customer_id` int NOT NULL,
        `ano_modelo` text,
        `modelo` text,
        `fabricante` text,
        `ano_veiculo` int DEFAULT NULL,
        `categoria` text,
        `dt_update` timestamp NULL DEFAULT NULL,
        PRIMARY KEY (`id`)
        )
        """
        )

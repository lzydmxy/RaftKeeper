import time
import pytest

import pymysql.cursors
from helpers.cluster import ClickHouseCluster

cluster = ClickHouseCluster(__file__)
node1 = cluster.add_instance('node1', with_odbc_drivers=True, with_mysql=True, image='alesapin/ubuntu_with_odbc:14.04')

create_table_sql_template =   """
    CREATE TABLE `clickhouse`.`{}` (
    `id` int(11) NOT NULL,
    `name` varchar(50) NOT NULL,
    `age` int  NOT NULL default 0,
    `money` int NOT NULL default 0,
    PRIMARY KEY (`id`)) ENGINE=InnoDB;
    """
def get_mysql_conn():
    conn = pymysql.connect(user='root', password='clickhouse', host='127.0.0.1', port=3308)
    return conn

def create_mysql_db(conn, name):
    with conn.cursor() as cursor:
        cursor.execute(
            "CREATE DATABASE {} DEFAULT CHARACTER SET 'utf8'".format(name))

def create_mysql_table(conn, table_name):
    with conn.cursor() as cursor:
        cursor.execute(create_table_sql_template.format(table_name))

@pytest.fixture(scope="module")
def started_cluster():
    try:
        cluster.start()

        conn = get_mysql_conn()
        ## create mysql db and table
        create_mysql_db(conn, 'clickhouse')

        yield cluster

    finally:
        cluster.shutdown()

def test_mysql_simple_select_works(started_cluster):
    mysql_setup = node1.odbc_drivers["MySQL"]

    table_name = 'test_insert_select'
    conn = get_mysql_conn()
    create_mysql_table(conn, table_name)

    node1.query('''
CREATE TABLE {}(id UInt32, name String, age UInt32, money UInt32) ENGINE = MySQL('mysql1:3306', 'clickhouse', '{}', 'root', 'clickhouse');
'''.format(table_name, table_name))

    node1.query("INSERT INTO {}(id, name, money) select number, concat('name_', toString(number)), 3 from numbers(100) ".format(table_name))

    # actually, I don't know, what wrong with that connection string, but libmyodbc always falls into segfault
    node1.query("SELECT * FROM odbc('DSN={}', '{}')".format(mysql_setup["DSN"], table_name), ignore_error=True)

    # server still works after segfault
    assert node1.query("select 1") == "1\n"

    conn.close()

def test_sqlite_simple_select_works(started_cluster):
    sqlite_setup = node1.odbc_drivers["SQLite3"]
    sqlite_db = sqlite_setup["Database"]

    node1.exec_in_container(["bash", "-c", "echo 'CREATE TABLE t1(x INTEGER PRIMARY KEY ASC, y, z);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
    node1.exec_in_container(["bash", "-c", "echo 'INSERT INTO t1 values(1, 2, 3);' | sqlite3 {}".format(sqlite_db)], privileged=True, user='root')
    assert node1.query("select * from odbc('DSN={}', '{}')".format(sqlite_setup["DSN"], 't1')) == "1\t2\t3\n"



import pandas as pd
import mysql.connector as msql
from mysql.connector import Error
import sys
import time
import configparser

def createDB():
    config = configparser.ConfigParser()
    config.read('config.ini')
    host = config.get('AWS RDS', 'host')
    user = config.get('AWS RDS', 'user')
    password = config.get('AWS RDS', 'password')

    try:
        conn = msql.connect(host=host, user=user, password=password)
        if conn.is_connected():
            cursor = conn.cursor()
            cursor.execute("CREATE DATABASE IF NOT EXISTS betzim_db")
            print("Database is created")
    except Error as e:
        print("Error while connecting to MySQL", e)

def connection():
    config = configparser.ConfigParser()
    config.read('config.ini')
    host = config.get('AWS RDS', 'host')
    user = config.get('AWS RDS', 'user')
    password = config.get('AWS RDS', 'password')

    conn = msql.connect(host=host, user=user, password=password)

    if conn.is_connected():
        cursor = conn.cursor()
        cursor.execute("USE betzim_db;")
        print("Connected to database: ")
    return conn, cursor

def dropAllTables(conn, cursor):
    if conn.is_connected():
        cursor.execute(f'DROP TABLE IF EXISTS document;')
        cursor.execute(f'DROP TABLE IF EXISTS trade;')
        cursor.execute(f'DROP TABLE IF EXISTS bet;')
        cursor.execute(f'DROP TABLE IF EXISTS user;')
        cursor.execute(f'DROP TABLE IF EXISTS country;')
        cursor.execute(f'DROP TABLE IF EXISTS contract;')
        cursor.execute(f'DROP TABLE IF EXISTS market;')
        cursor.execute(f'DROP TABLE IF EXISTS event;')
        cursor.execute(f'DROP TABLE IF EXISTS category;')
        
def createTable(table):
    match table:
        case "country":
            return "CREATE TABLE country(\
                id varchar(255) NOT NULL,\
                name text NOT NULL,\
                PRIMARY KEY (id))"
        case "user":
            return "CREATE TABLE user(\
                id int NOT NULL,\
                name text NOT NULL,\
                address text NOT NULL,\
                date text NOT NULL,\
                phone text NOT NULL,\
                email text NOT NULL,\
                excluded boolean NOT NULL,\
                approved boolean NOT NULL,\
                balance double NOT NULL,\
                countryId varchar(255),\
                PRIMARY KEY (id),\
                FOREIGN KEY (countryId) REFERENCES country(id))"
        case "document":
            return "CREATE TABLE document(\
                id int NOT NULL,\
                type text NOT NULL,\
                file text NOT NULL,\
                approved text NOT NULL,\
                userId int NOT NULL,\
                PRIMARY KEY (id),\
                FOREIGN KEY (userId) REFERENCES user(id))"
        case "category":
            return "CREATE TABLE category(\
                name varchar(255) NOT NULL,\
                PRIMARY KEY (name))"
        case "event":
            return "CREATE TABLE event(\
                id int,\
                name text NOT NULL,\
                startTime text NOT NULL,\
                endTime text NOT NULL,\
                category VARCHAR(255) NOT NULL,\
                PRIMARY KEY (id),\
                FOREIGN KEY (category) REFERENCES category(name))"
        case "market":
            return "CREATE TABLE market(\
                id int,\
                event_id int NOT NULL,\
                name text NOT NULL,\
                PRIMARY KEY (id),\
                FOREIGN KEY (event_id) REFERENCES event(id))"
        case "contract":
            return "CREATE TABLE contract(\
                id int,\
                market_id int NOT NULL,\
                name text NOT NULL,\
                winner int NOT NULL,\
                CONSTRAINT contract_pk PRIMARY KEY (id, market_id),\
                FOREIGN KEY (market_id) REFERENCES market(id))"
        case "trade":
            return "CREATE TABLE trade(\
                id int,\
                back_bet_id int NOT NULL,\
                lay_bet_id int NOT NULL,\
                odd double NOT NULL,\
                value double NOT NULL,\
                PRIMARY KEY (id),\
                FOREIGN KEY (back_bet_id) REFERENCES bet(id),\
                FOREIGN KEY (lay_bet_id) REFERENCES bet(id))"
        case "bet":
            return "CREATE TABLE bet(\
                id int,\
                market_id int NOT NULL,\
                contract_id int NOT NULL,\
                user_id int NOT NULL,\
                odd double NOT NULL,\
                value double NOT NULL,\
                type text NOT NULL,\
                PRIMARY KEY (id),\
                CONSTRAINT CONTRACT_ID FOREIGN KEY(contract_id, market_id) REFERENCES contract(id, market_id),\
                CONSTRAINT USER_ID FOREIGN KEY (user_id) REFERENCES user(id))"

def insertValues(name):
    match name:
        case "country":
            return "INSERT INTO betzim_db.country VALUES (%s,%s)"
        case "user":
            return "INSERT INTO betzim_db.user VALUES (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)"
        case "document":
            return "INSERT INTO betzim_db.document VALUES (%s,%s,%s,%s,%s)"
        case "category":
            return "INSERT INTO betzim_db.category VALUES (%s)"
        case "event":
            return "INSERT INTO betzim_db.event VALUES (%s,%s,%s,%s,%s)"
        case "market":
            return "INSERT INTO betzim_db.market VALUES (%s,%s,%s)"
        case "contract":
            return "INSERT INTO betzim_db.contract VALUES (%s,%s,%s,%s)"
        case "trade":
            return "INSERT INTO betzim_db.trade VALUES (%s,%s,%s,%s,%s)"
        case "bet":
            return "INSERT INTO betzim_db.bet VALUES (%s,%s,%s,%s,%s,%s,%s)"

def getTableValues(table):
    values = table.values.tolist()
    tuples = []
    for x in values:
        tuples.append(tuple(x))
    return values

def importTable(conn, cursor, table_name):
    start_time = time.time()
    print(f":::::::::: {table_name} table :::::::::")
    table = pd.read_csv(f'./data/{table_name}.csv', index_col=False, delimiter = ',', encoding = "ISO-8859-1")
    lst = getTableValues(table)
    if conn.is_connected():
        print('Creating table')
        cursor.execute(createTable(table_name))
        print("Table created")
        print("Inserting records")
        sql = insertValues(table_name)
        cursor.executemany(sql, lst)
        conn.commit()
        print("Records inserted")
    end_time = time.time()
    print(f"Duration: {end_time-start_time} seconds")

def main():
    createDB()
    conn, cursor = connection()

    dropAllTables(conn, cursor)

    tables = ["country", "user", "document", "category", "event", "market", "contract", "bet", "trade"]

    for table in tables:
        importTable(conn, cursor, table) 

if __name__ == "__main__":
    main()
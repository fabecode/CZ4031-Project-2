import psycopg2
import configparser
import json


class Database:
    def __init__(self):
        """
        Starts database connection
        """
        self.config = configparser.ConfigParser()
        self.config.read('database.ini')
        self.conn = psycopg2.connect(
            host=self.config['postgresql']['host'],
            database=self.config['postgresql']['database'],
            user=self.config['postgresql']['user'],
            password=self.config['postgresql']['password'],
            port=self.config['postgresql']['port']
        )
        self.cursor = self.conn.cursor()

    def query(self, query):
        """
        Executes query and returns the qep. For debugging purpose will store to json for now.
        :param query: SQL query to execute
        :return: QEP
        """
        self.cursor.execute("EXPLAIN (FORMAT JSON)" + query)
        qep = self.cursor.fetchall()[0][0][0]
        with open('data.json', 'w', encoding='utf-8') as f:
            json.dump(qep, f, ensure_ascii=False, indent=4)
        return qep

    def closeConnection(self):
        """
        Close connection to database
        :return: None
        """
        self.cursor.close()
        self.conn.close()

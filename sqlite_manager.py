import sqlite3
import datetime


class SqliteManager:
    def __init__(self):
        self.database = r"C:\dev\pymachine\databases\cryptocurrency.db"

    def create_table(self, table=None, columns=None):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        with connection:
            command = "DROP TABLE IF EXISTS {table}".format(table=table)
            cursor.execute(command)

            command = "CREATE TABLE {table}({columns})".format(table=table, columns=columns)
            cursor.execute(command)

    def insert_data(self, table=None, data=None):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        with connection:
            string = self.__build_values_string(data=data)
            data = (tuple(data),)
            command = "INSERT INTO {table} VALUES({string})".format(table=table, string=string)
            cursor.executemany(command, data)

    def get_tables_in_database(self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        with connection:
            command = "SELECT name FROM sqlite_master WHERE type='table'"
            cursor.execute(command)

            tables = cursor.fetchall()
            
            prepared_tables = []
            for table in tables:
                prepared_tables.append(str(table[0]))
            return prepared_tables

    def retrieve_from_table(self, row="*", table=None):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        with connection:
            command = "SELECT {row} FROM {table}".format(row=row, table=table)
            cursor.execute(command)

            return cursor.fetchall()

    def commit_to_database(self):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        connection.commit()

    def get_last_id_index(self, table=None):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        with connection:
            command = "SELECT id FROM {table}".format(table=table)
            cursor.execute(command)

            row_id = cursor.fetchall()

            if len(row_id) == 0:
                row_id = 0
            else:
                row_id = max(row_id)[0]

            return row_id

    def __build_values_string(self, data=None):
        symbol = ", "
        holders = []
        for value in data:
            holders.append("?")

        return symbol.join(holders)

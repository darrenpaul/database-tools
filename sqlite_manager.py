import sqlite3
import datetime


class SqliteManager:
    def __init__(self):
        self.database = r"C:\dev\pymachine\databases\cryptocurrency.db"

    def create_table(self, table, columns):
        """
        Create a new table in your database along with all the columns
        
        Keyword Arguments:
            table {basestring} -- The name for the table being created
            columns {basestring} -- A string the the name and type of for all the columns
            E.G. <name type, name type>
        """

        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        with connection:
            command = "DROP TABLE IF EXISTS {table}".format(table=table)
            cursor.execute(command)

            command = "CREATE TABLE {table}({columns})".format(table=table, columns=columns)
            cursor.execute(command)

    def insert_data(self, table=None, data=None):
        """
        Insert data into the define table
        
        Keyword Arguments:
            table {basestring} -- The name of the table to insert into
            data {list} -- A list of all the data in the correct order of columns
            E.G. <[item1, item2, item3, item4]>
        """

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

    def retrieve_from_table(self, row="*", table=None, where=None):
        # todo fix return docstring
        """
        Retrieve all the data from the table and the specifed row
        
        Keyword Arguments:
            row {basestring} -- The row now to use (default: {"*"})
            table {basestring} -- The name of the table to query from
            where {basestring} -- The search condition to use E.G. <name=apples>
        
        Returns:
            [type] -- [description]
        """
        if table:
            connection = sqlite3.connect(self.database)
            cursor = connection.cursor()

            with connection:
                if where:
                    command = "SELECT {row} FROM {table} WHERE {where}".format(row=row, table=table, where=where)
                else:
                    command = "SELECT {row} FROM {table}".format(row=row, table=table)
                cursor.execute(command)

                return cursor.fetchall()

    def commit_to_database(self):
        connection = sqlite3.connect(self.database)

        connection.commit()

    def get_last_id_index(self, table):
        """
        This gets the last value under the id row
        
        Keyword Arguments:
            table {basestring} -- The name of the table to query from
        
        Returns:
            basestring -- returns the last value
        """

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
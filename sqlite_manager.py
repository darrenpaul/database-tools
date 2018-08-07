import os
import sqlite3
import datetime
from pprint import pprint

ROOT_DIR = os.path.dirname(os.path.abspath(__file__))

class SqliteManager:
    def __init__(self):
        self.database = os.path.join(ROOT_DIR, "database.sqlite")

    def create_database(self, path=None):
        if path is None:
            path = self.database
        connection = sqlite3.connect(path)
        cursor = connection.cursor()
        connection.commit()
        connection.close()

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
            print command
            cursor.execute(command)

    def create_table_column_string(self, key, type="text", allow_null=False, unique=False, primary_key=False, auto_increment=False, foreign_key=[]):
        """
        Create a new table in your database along with all the columns

        Keyword Arguments:
            key {basestring} -- The name of the column
            type {basestring} -- The Field type of the column
            allow_null {bool} -- Allow fields to be null
            unique {bool} -- Makes the value unique
            primary_key {bool} -- Will this column be a primary key
            foreign_key {list} -- A list of the column and table. The first value will be the column name, the second value will be the table name
                E.G. <[column, table]>
        """
        _string = str(key)
        _fieldType = self.__convert_to_sql_field_type(string=type)
        if _fieldType is not False:
            _string = _string + " " + _fieldType
        
            if allow_null is False:
                _string = _string + " NOT NULL"
            
            if unique:
                _string = _string + " UNIQUE"

            if primary_key is True:
                _string = _string + " PRIMARY KEY"
                if _fieldType == "INT":
                    if auto_increment is True:
                        _string = _string + " AUTOINCREMENT"

            foreign_key = self.__parse_foreign_keys(data=foreign_key)
            if foreign_key is not False:
                _string = _string + ", " + foreign_key

        return _string

    def enable_foreign_keys(self, state=True):
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()
        cursor.execute("pragma foreign_keys=ON")

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

    def update_data(self, **kwargs):
        """
        Update already existing data within the database

        Keyword Arguments:
            table {basestring} -- The name of the table to insert into
            set {basestring} -- The updated value E.G. <name=apples>
            where {basestring} -- The search condition to use E.G. <name=apples>
        :param kwargs: <table>, <setvalue>, <where>
        :return:
        """
        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        with connection:
            command = "UPDATE {table} SET {setvalue} WHERE {where}".format(table=kwargs["table"],
                                                                           setvalue=kwargs["setvalue"],
                                                                           where=kwargs["where"])

            cursor.execute(command)

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
                keys = self.__get_row_names(data=cursor.description)
                values = cursor.fetchall()

                data = self.__format_data(rows=keys, values=values)

                return data

    def commit_to_database(self):
        connection = sqlite3.connect(self.database)

        connection.commit()

    def get_last_column_value(self, table, column_name):
        """
        This gets the last value under the id row

        Keyword Arguments:
            table {basestring} -- The name of the table to query from
            column_name {basestring} -- The name of the column in the table

        Returns:
            basestring -- returns the last value
        """

        connection = sqlite3.connect(self.database)
        cursor = connection.cursor()

        with connection:
            command = "SELECT {column_name} FROM {table}".format(column_name=column_name, table=table)
            cursor.execute(command)

            row_id = cursor.fetchall()

            if len(row_id) == 0:
                row_id = 0
            else:
                row_id = max(row_id)[0]

            return row_id

    @staticmethod
    def prepare_set_value(**kwargs):
        """
        Provide a dictionary of the row name in the dictionary and the value to update
        :param kwargs:
        :return:
        """
        string = ""
        for key, val in kwargs.iteritems():
            string = "{string}{key}='{value}', ".format(string=string, key=key, value=val)
        string = string[:-2]
        return string

    @staticmethod
    def __convert_to_sql_field_type(string):
        _data = {
            "text": "TEXT",
            "integer": "INTEGER"
            }
        if string in _data.keys():
            return _data[string]
        return False

    @staticmethod
    def __parse_foreign_keys(data):
        if len(data) == 2:
            return "FOREIGN KEY({key}) REFERENCES {table}({key})".format(key=data[0], table=data[1])
        return False

    def __build_values_string(self, data=None):
        symbol = ", "
        holders = []
        for value in data:
            holders.append("?")

        return symbol.join(holders)

    def __get_row_names(self, data):
        keys = []
        for item in data:
            keys.append(item[0])
        return keys

    def __format_data(self, rows, values):
        data = []
        for value in values:
            d = {}
            for i in range(0, len(rows)):
                d.update({rows[i]: value[i]})
            data.append(d)

        return data

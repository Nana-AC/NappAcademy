import sqlite3
from decouple import config

class Model:
    table_name = None
    db_name = config("DB_NAME")

    def __init__(self):
        assert self.table_name is not None, "Table name cannot be empty"
        self.conn = sqlite3.connect(self.db_name)
        self.cursor = self.conn.cursor()

        self.init_db()

    def init_db(self):
        self.cursor.execute(self._create_table_query())
        self.commit()

    def _insert_query(self):
        raise NotImplementedError()

    def insert(self, data):
        cursor_execute = self.cursor.execute
        if isinstance(data, (list, tuple)):
            cursor_execute = self.cursor.executemany
        
        cursor_execute(self._insert_query(), data)
        self.commit()

    def _create_table_query(self):
        raise NotImplementedError()

    def commit(self):
        self.conn.commit()


class Character(Model):
    table_name = "character"

    def _create_table_query(self):
        return f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER NOT NULL PRIMARY KEY,
                name TEXT NOT NULL,
                description TEXT NOT NULL,
                thumbnail TEXT NOT NULL
            );
        """

    def _insert_query(self):
        return f"""
        INSERT OR REPLACE INTO {self.table_name} (id, name, description, thumbnail)
        VALUES (?, ?, ?, ?);
        """


class Comic(Model):
    table_name = "comic"

    def _create_table_query(self):
        return f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                personagem_id INTEGER,
                FOREIGN KEY(personagem_id) REFERENCES character(id)
            );
        """

    def _insert_query(self):
        return f"""
        INSERT OR REPLACE INTO {self.table_name} (personagem_id, name)
        VALUES (?, ?);
        """


class Story(Model):
    table_name = "story"

    def _create_table_query(self):
        return f"""
            CREATE TABLE IF NOT EXISTS {self.table_name} (
                id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                name TEXT NOT NULL,
                personagem_id INTEGER,
                FOREIGN KEY(personagem_id) REFERENCES character(id)
            );
        """

    def _insert_query(self):
        return f"""
        INSERT OR REPLACE INTO {self.table_name} (personagem_id, name)
        VALUES (?, ?);
        """

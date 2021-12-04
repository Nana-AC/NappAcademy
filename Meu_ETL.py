import datetime
import hashlib
import sqlite3
from sqlite3 import OperationalError

import requests
from decouple import config


class MeuETL:
    TIMESTAMP = datetime.datetime.now().strftime("%Y-%m-%d%H:%M:%S")
    PUB_KEY = config("PUB_KEY")
    PRIV_KEY = config("PRIV_KEY")

    def __init__(self):
        self.result = self.results()
        self.com = self.comics()
        self.stor = self.stories()

    def hash_params(self):
        hash_md5 = hashlib.md5()
        hash_md5.update(
            f"{self.TIMESTAMP}{self.PRIV_KEY}{self.PUB_KEY}".encode("utf-8")
        )
        hashed_params = hash_md5.hexdigest()
        return hashed_params

    def results(self):
        params = {
            "ts": self.TIMESTAMP,
            "apikey": self.PUB_KEY,
            "hash": self.hash_params(),
            "offset": 0,
            "limit": 100,
        }
        res = requests.get(
            "https://gateway.marvel.com:443/v1/public/characters", params=params
        ).json()
        personagens = res["data"]["results"]
        total = res["data"]["total"]
        for x in range(100, total + 100, 100):
            params["offset"] = x
            res = requests.get(
                "https://gateway.marvel.com:443/v1/public/characters", params=params
            ).json()
            personagens.extend(res["data"]["results"])
        return personagens

    def comics(self):
        comics_list = []
        for result in self.result:
            personagem_id = result["id"]
            comics = result["comics"]["items"]
            for comic in comics:
                comics_list.append((personagem_id, comic["name"]))
        return comics_list

    def stories(self):
        stories_list = []
        for result in self.result:
            personagem_id = result["id"]
            stories = result["stories"]["items"]
            for storie in stories:
                stories_list.append((personagem_id, storie["name"]))
        return stories_list

    def characters(self):
        characters_list = []
        for character in self.result:
            id = character["id"]
            nome = character["name"]
            descricao = character["description"]
            path = character["thumbnail"]["path"]
            extensao = character["thumbnail"]["extension"]
            foto = f"{path}.{extensao}"
            characters_list.append((id, nome, descricao, foto))
        return characters_list

    def extract_transform(self):
        comics = self.comics()
        stories = self.stories()
        characters = self.characters()
        self.load(characters, comics, stories)

    def load(self, lista_marvel, lista_comics, lista_stories):
        self.criar_banco_marvel()
        self.conn = sqlite3.connect("marvel.db")
        self.cursor = self.conn.cursor()

        self.cursor.executemany(
            """
        INSERT OR REPLACE INTO character (id, name, description, thumbnail)
        VALUES (?,?,?,?);
        """,
            lista_marvel,
        )
        self.cursor.executemany(
            """
        INSERT OR REPLACE INTO comics (personagem_id, name)
        VALUES (?,?);
        """,
            lista_comics,
        )
        self.cursor.executemany(
            """
        INSERT OR REPLACE INTO stories (personagem_id, name)
        VALUES (?,?);
        """,
            lista_stories,
        )
        self.conn.commit()
        self.conn.close()

    def criar_banco_marvel(self):
        self.conn = sqlite3.connect("marvel.db")
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute(
                """
                    CREATE TABLE character (
                        id INTEGER NOT NULL PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT NOT NULL,
                        thumbnail TEXT NOT NULL );
                        """
            )
            self.cursor.execute(
                """
                    CREATE TABLE comics (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        personagem_id INTEGER, 
                        FOREIGN KEY(personagem_id) REFERENCES character(id) );
                        """
            )
            self.cursor.execute(
                """
                    CREATE TABLE stories (
                        id INTEGER NOT NULL PRIMARY KEY AUTOINCREMENT,
                        name TEXT NOT NULL,
                        personagem_id INTEGER, 
                        FOREIGN KEY(personagem_id) REFERENCES character(id) );
                        """
            )
        except OperationalError:
            print("Tabelas criadas!")
        self.conn.close()


if __name__ == "__main__":
    meu_etl = MeuETL()
    meu_etl.extract_transform()

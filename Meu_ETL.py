import hashlib
import requests
import datetime
import sqlite3
from sqlite3 import OperationalError
from decouple import config 

class MeuETL:
    TIMESTAMP = datetime.datetime.now().strftime('%Y-%m-%d%H:%M:%S')
    PUB_KEY = config('PUB_KEY')
    PRIV_KEY = config('PRIV_KEY')

    def __init__(self):
        self.chars = self.characters()

    def hash_params(self):
        hash_md5 = hashlib.md5()
        hash_md5.update(f'{self.TIMESTAMP}{self.PRIV_KEY}{self.PUB_KEY}'.encode('utf-8'))
        hashed_params = hash_md5.hexdigest()

        return hashed_params

    def characters(self):
        params = {'ts': self.TIMESTAMP, 'apikey': self.PUB_KEY, 'hash': self.hash_params(), 'offset': 0, 'limit': 100}
        res = requests.get('https://gateway.marvel.com:443/v1/public/characters',
                        params=params).json()
        personagens = res['data']['results']
        total = res['data']['total']
        for x in range(100, total + 100, 100):
            params['offset'] = x   
            res = requests.get('https://gateway.marvel.com:443/v1/public/characters',
                        params=params).json()
            personagens.extend(res['data']['results'])
        return personagens

    def extract_transform(self):
        lista_marvel = []
        for character in self.chars:
            id = character['id']
            nome = character['name']
            descricao = character['description']
            path = character['thumbnail']['path']
            extensao = character['thumbnail']['extension']
            foto = f"{path}.{extensao}"
            lista_marvel.append((id, nome, descricao, foto))
        self.load(lista_marvel)

    def load(self, lista_marvel):
        self.criar_banco_marvel()
        self.conn = sqlite3.connect('marvel.db')
        self.cursor = self.conn.cursor()

        self.cursor.executemany("""
        INSERT OR REPLACE INTO character (id, name, description, thumbnail)
        VALUES (?,?,?,?)
        """, lista_marvel)
        self.conn.commit()
        self.conn.close()

    def criar_banco_marvel(self):
        self.conn = sqlite3.connect('marvel.db')
        self.cursor = self.conn.cursor()
        try:
            self.cursor.execute("""
                    CREATE TABLE character (
                        id INTEGER NOT NULL PRIMARY KEY,
                        name TEXT NOT NULL,
                        description TEXT NOT NULL,
                        thumbnail TEXT NOT NULL);
                        """)
        except OperationalError:
            print('Tabela já criada')
        self.conn.close()

if __name__ == "__main__":
    meu_etl = MeuETL()
    meu_etl.extract_transform()









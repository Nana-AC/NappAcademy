import datetime
import hashlib
import sqlite3
from sqlite3 import OperationalError
import grequests
from models import Character, Comic, Story

import requests
from decouple import config


class MeuETL:
    TIMESTAMP = datetime.datetime.now().strftime("%Y-%m-%d%H:%M:%S")
    PUB_KEY = config("PUB_KEY")
    PRIV_KEY = config("PRIV_KEY")

    def __init__(self):
        # self.result = self.results()
        self.result = self.results_v2()
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
        print('Starting results')
        params = {
            "ts": self.TIMESTAMP,
            "apikey": self.PUB_KEY,
            "hash": self.hash_params(),
            "offset": 0,
            "limit": 100,
        }
        import time
        for i in range(1, 15):
            start = time.time()

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
            print(f"results - Rodada {i} - {time.time() - start:.2f}s")
        return personagens

    def results_v2(self):
        print('Starting results v2')
        params = {
            "ts": self.TIMESTAMP,
            "apikey": self.PUB_KEY,
            "hash": self.hash_params(),
            "offset": 0,
            "limit": 100,
        }

        import time
        start = time.time()
        res = requests.get(
            "https://gateway.marvel.com:443/v1/public/characters", params=params
        ).json()
        personagens = res["data"]["results"]
        total = res["data"]["total"]
        rs = [
            grequests.get(
                "https://gateway.marvel.com:443/v1/public/characters",
                params=params.update(offset=offset) or dict(params),
            )
            for offset in range(100, total, 100)
        ]

        for res in grequests.map(rs):
            try:
                personagens.extend(res.json()["data"]["results"])
            except Exception as e:
                print(res.json())
                print(e)
        print(f"results - Rodada {0} - {time.time() - start:.2f}s")

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
        Character().insert(lista_marvel)
        Comic().insert(lista_comics)
        Story().insert(lista_stories)


if __name__ == "__main__":
    meu_etl = MeuETL()
    meu_etl.extract_transform()

#!/usr/bin/env python3

from typing import Generator
import cachecontrol
import pandas as pd
from pandas.core.frame import DataFrame
import requests
from cachecontrol import CacheControl


class MovieDatabaseServer:
    db_url = "https://movie-database-server.appspot.com/"
    sizes = {
        "small": "database_small",
        "medium": "database_medium",
        "large": "database_large",
    }

    def get_database(self, size: str, caching=True) -> dict:
        base_sess = requests.session()
        cached_sess = CacheControl(base_sess)
        session = cached_sess if caching else base_sess

        response = session.get(self.db_url + self.sizes[size])
        response.raise_for_status()
        return response.json()

    def _movies_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(
            self.get_database("small")["movies"], orient="index"
        )

    @property
    def movie_titles(self) -> pd.DataFrame:
        df = self._movies_dataframe()
        return df["id", "title"]

    @property
    def movies_relational_dataframe(self) -> pd.DataFrame:
        df = self._movies_dataframe()
        df.reset_index(inplace=True)
        df = df[["index", "actors"]]
        df = df.explode("actors")
        df.set_index("index", inplace=True)
        return df

    @property
    def actors_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(
            self.get_database("small")["actors"], orient="index"
        )
        df.reset_index(inplace=True)
        df.columns = ["index", "actor"]
        return df

    def get_actors_movie_ids(self, actor_id) -> Generator[str, None, None]:
        """
        Returns first movie ID matching a given actor
        """
        actor_id = str(actor_id)
        movies = self.movies_relational_dataframe[
            self.movies_relational_dataframe["actors"] == actor_id
        ].reset_index()
        yield from movies["index"]

    def get_movie_title_by_id(self, id) -> str:
        return self.movies_relational_dataframe.loc[["id"] == id]["title"]


def get_kevin_bacon_id(server: MovieDatabaseServer) -> str:
    return server.actors_dataframe[
        server.actors_dataframe["actor"] == "Kevin Bacon"
    ].iloc[0]["index"]


def get_kevin_bacon_movie(server: MovieDatabaseServer) -> str:
    kevin_bacon_id = "4724"
    movie_id = next(server.get_actors_movie_ids(kevin_bacon_id))
    return server.get_movie_title_by_id(movie_id)


movie_db_instance = MovieDatabaseServer()
print(get_kevin_bacon_id(movie_db_instance))
print(get_kevin_bacon_movie(movie_db_instance))

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

    def __init__(self, size="small") -> None:
        self.size = size

    def get_database(self, caching=True) -> dict:
        base_sess = requests.session()
        cached_sess = CacheControl(base_sess)
        session = cached_sess if caching else base_sess

        response = session.get(self.db_url + self.sizes[self.size])
        response.raise_for_status()
        return response.json()

    def _movies_dataframe(self) -> pd.DataFrame:
        return pd.DataFrame.from_dict(self.get_database()["movies"], orient="index")

    @property
    def movie_titles(self) -> pd.DataFrame:
        df = self._movies_dataframe()
        df.reset_index(inplace=True)
        return df[["index", "title"]]

    @property
    def movies_relational_dataframe(self) -> pd.DataFrame:
        df = self._movies_dataframe()
        df.reset_index(inplace=True)
        df = df[["index", "actors"]]
        df = df.explode("actors")
        df.rename(columns={"actors": "actor"}, inplace=True)
        df["actor"] = df["actor"].astype(str)
        # df.set_index("index", inplace=True)
        return df

    @property
    def actors_dataframe(self) -> pd.DataFrame:
        df = pd.DataFrame.from_dict(self.get_database()["actors"], orient="index")
        df.reset_index(inplace=True)
        df.columns = ["index", "actor"]
        return df

    def get_actors_movie_ids(self, actor_id) -> Generator[str, None, None]:
        """
        Returns first movie ID matching a given actor
        """
        actor_id = str(actor_id)
        movies = self.movies_relational_dataframe.loc[
            self.movies_relational_dataframe["actor"] == actor_id
        ]
        yield from movies["index"]

    def get_movie_title_by_id(self, index) -> str:
        movie_row = self.movie_titles.loc[self.movie_titles["index"] == index].iloc[0]
        return movie_row["title"]

    def get_actor_id_by_name(self, full_name) -> str:
        return self.actors_dataframe[self.actors_dataframe["actor"] == full_name].iloc[
            0
        ]["index"]


server = MovieDatabaseServer()

kb_id = server.get_actor_id_by_name("Kevin Bacon")
print(kb_id)


kb_movie_id = next(server.get_actors_movie_ids(kb_id))
kb_movie_title = server.get_movie_title_by_id(kb_movie_id)
print(f"Kevin Bacon has starred in {kb_movie_title}.")

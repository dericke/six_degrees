#!/usr/bin/env python3

import json
import sys
from typing import Generator, List, Optional, Set

import networkx as nx
import pandas as pd
import requests
from cachecontrol import CacheControl
from requests.models import HTTPError
from requests.sessions import HTTPAdapter


class MovieDatabaseServer:
    db_url = "https://movie-database-server.appspot.com/"
    sizes = {
        "small": "database_small",
        "medium": "database_medium",
        "large": "database_large",
    }

    def __init__(self, size="small") -> None:
        self.size = size
        self.graph = nx.Graph()

        try:
            with open(f"data/{self.sizes[self.size]}.json") as f:
                self.raw_database = json.load(f)
        except FileNotFoundError:
            try:
                self.raw_database = self.download_database()
            except (HTTPError, ConnectionError):
                print(
                    "Could not obtain a suitable database either locally or online, exiting."
                )
                sys.exit(1)

    def get_database(self):
        return self.raw_database

    def download_database(self, caching=True) -> dict:
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

    def get_actors_movie_ids(self, actor_id: str) -> Generator[str, None, None]:
        """
        Returns first movie ID matching a given actor
        """
        actor_id = str(actor_id)
        movies = self.movies_relational_dataframe.loc[
            self.movies_relational_dataframe["actor"] == actor_id
        ]
        yield from movies["index"]

    def get_movie_title_by_id(self, index: str) -> str:
        movie_row = self.movie_titles.loc[self.movie_titles["index"] == index].iloc[0]
        return movie_row["title"]

    def get_actor_id_by_name(self, full_name: str) -> str:
        return self.actors_dataframe[self.actors_dataframe["actor"] == full_name].iloc[
            0
        ]["index"]

    def get_actor_name_by_id(self, actor_id: str) -> str:
        return self.actors_dataframe[self.actors_dataframe["index"] == actor_id].iloc[
            0
        ]["actor"]

    def get_costar_ids(self, actor_id: str) -> Set[str]:
        costars = set()
        for movie_id in self.get_actors_movie_ids(actor_id):
            movie_stars = {
                movie_star_id
                for movie_star_id in self.movies_relational_dataframe.loc[
                    self.movies_relational_dataframe["index"] == movie_id
                ]["actor"]
                if movie_star_id != actor_id
            }
            costars |= movie_stars
        return costars

    def build_actor_connection_graph(self) -> nx.Graph:
        indexed_movies = self.movies_relational_dataframe[["actor", "index"]]
        indexed_movies.rename(columns={"index": "movie"}, inplace=True)

        # data_to_merge = indexed_movies.drop_duplicates()
        indexed_movies = indexed_movies.merge(
            indexed_movies.rename(columns={"actor": "actor_2"}), on="movie"
        )
        indexed_movies = indexed_movies[
            ~(indexed_movies["actor"] == indexed_movies["actor_2"])
        ].dropna()[["actor", "actor_2", "movie"]]

        graph = nx.from_pandas_edgelist(
            indexed_movies, source="actor", target="actor_2", edge_attr="movie"
        )
        graph.add_nodes_from(nodes_for_adding=list(self.actors_dataframe["index"]))
        return graph

    def get_degrees_of_separation(
        self, actor1_id: str, actor2_id: str
    ) -> Optional[int]:
        path = nx.shortest_path(
            self.build_actor_connection_graph(), actor1_id, actor2_id
        )
        if not path:
            # No connection
            return None
        return len(path) - 1


def get_kbn(database: MovieDatabaseServer, other_actor_name) -> int:
    actor2_id = database.get_actor_id_by_name(other_actor_name)
    return database.get_degrees_of_separation()


def get_kb_id(database: MovieDatabaseServer) -> str:
    return database.get_actor_id_by_name("Kevin Bacon")


def get_kb_movie_title(database: MovieDatabaseServer) -> str:
    kb_movie_id = next(database.get_actors_movie_ids(kb_id))
    return database.get_movie_title_by_id(kb_movie_id)


def get_kb_costars(database: MovieDatabaseServer) -> List[str]:
    kb_costar_ids = database.get_costar_ids(kb_id)
    return [database.get_actor_name_by_id(actor_id) for actor_id in kb_costar_ids]


database = MovieDatabaseServer("small")


kb_id = get_kb_id(database)
print(kb_id)


kb_movie_title = get_kb_movie_title(database)
print(f"Kevin Bacon has starred in {kb_movie_title}.")


print("Kevin Bacon has co-starred with these actors:")
for star in get_kb_costars(database):
    print(f"    {star}")

other_actor = "Tom Hanks"
other_actor_id = database.get_actor_id_by_name(other_actor)

kbn = database.get_degrees_of_separation(kb_id, other_actor_id)

print(f"{other_actor} has {kbn} degrees of separation from Kevin Bacon")

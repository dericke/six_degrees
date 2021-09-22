#!/usr/bin/env python3

import cachecontrol
import pandas
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

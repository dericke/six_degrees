import pytest

from movie_query.main import MovieDatabaseServer

movie_db_instance = MovieDatabaseServer()


@pytest.mark.parametrize("size", ["small", "medium", "large"])
def test_get_database(size):
    db_json = movie_db_instance.get_database(size)
    assert set(db_json.keys()) == {"movies", "actors"}


def test_movies_dataframe():
    df = movie_db_instance.movies_relational_dataframe
    pass


def test_actors_dataframe():
    df = movie_db_instance.actors_dataframe
    pass


@pytest.mark.parametrize(
    "actor_id,full_name,kevin_bacon_number",
    [
        # small
        [41043, "Tim McInnerny", 4],
        [2155, "Thora Birch", 5],
        # medium
        # [31, "Tom Hanks", 1],
        # [163985, "Manny Alfaro", 3]
        # large
        # [5, "Peter Cushing", 2],
        # [1572923, "Milad Hossein Pour", 5],
    ],
)
def test_kevin_bacon_number(actor_id: int, full_name: str, kevin_bacon_number: int):
    db = movie_db_instance.get_database("small")
    pass

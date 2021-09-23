import pytest

from movie_query.main import MovieDatabaseServer

movie_db_instance = MovieDatabaseServer("small")


@pytest.mark.parametrize(
    "actor_id,full_name,kevin_bacon_number",
    [
        # small
        ["41043", "Tim McInnerny", 4],
        ["2155", "Thora Birch", 5],
        # medium
        # [31, "Tom Hanks", 1],
        # [163985, "Manny Alfaro", 3]
        # large
        # [5, "Peter Cushing", 2],
        # [1572923, "Milad Hossein Pour", 5],
    ],
)
def test_get_id_by_name(actor_id: str, full_name: str, kevin_bacon_number: int):
    assert movie_db_instance.get_actor_id_by_name(full_name) == actor_id


@pytest.mark.parametrize(
    "actor_id,full_name,kevin_bacon_number",
    [
        # small
        ["41043", "Tim McInnerny", 4],
        ["2155", "Thora Birch", 5],
        # medium
        # [31, "Tom Hanks", 1],
        # [163985, "Manny Alfaro", 3]
        # large
        # [5, "Peter Cushing", 2],
        # [1572923, "Milad Hossein Pour", 5],
    ],
)
def test_kevin_bacon_number(actor_id: str, full_name: str, kevin_bacon_number: int):
    assert (
        movie_db_instance.get_degrees_of_separation("4724", actor_id)
        == kevin_bacon_number
    )

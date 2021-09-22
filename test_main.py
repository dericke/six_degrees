import pytest


@pytest.mark.parametrize(
    "actor_id,full_name,kevin_bacon_number",
    [
        # small
        [41043, "Tim McInnerny", 4][2155, "Thora Birch", 5],
        # medium
        [31, "Tom Hanks", 1],
        [163985, "Manny Alfaro", 3]
        # large
        [5, "Peter Cushing", 2],
        [1572923, "Milad Hossein Pour", 5],
    ],
)
def test_kevin_bacon_number(actor_id: int, full_name: str, kevin_bacon_number: int):
    pass

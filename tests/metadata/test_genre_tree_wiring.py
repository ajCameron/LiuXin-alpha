"""Tests for the fiction branch->leaf genre wiring.

These tests target the new classifier in LiuXin_alpha.metadata.standardize_genre,
re-exported via LiuXin_alpha.metadata.standardization for convenience.
"""


def test_classify_fiction_genre_scifi_space_opera() -> None:
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Space Opera")
    assert r.branch == "Science Fiction"
    assert r.leaf == "Space Opera"


def test_classify_fiction_genre_scifi_time_travel() -> None:
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("sci fi / time travel")
    assert r.branch == "Science Fiction"
    assert r.leaf == "Time Travel"


def test_classify_fiction_genre_fantasy_urban() -> None:
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Urban fantasy")
    assert r.branch == "Fantasy"
    assert r.leaf == "Urban Fantasy"


def test_classify_fiction_genre_romance_romantasy() -> None:
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Romantasy")
    assert r.branch == "Romance"
    assert r.leaf == "Fantasy Romance"


def test_classify_fiction_genre_mystery_cozy() -> None:
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Cozy Mystery")
    assert r.branch == "Mystery/Crime/Thriller"
    assert r.leaf == "Cozy Mystery"


def test_classify_fiction_genre_horror_gothic() -> None:
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Gothic Horror")
    assert r.branch == "Horror"
    assert r.leaf == "Gothic Horror"


def test_classify_fiction_genre_literary_litfic() -> None:
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("lit fic")
    assert r.branch == "Literary & General"
    assert r.leaf == "Literary Fiction"


def test_classify_fiction_genre_genre_treasure_hunt() -> None:
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Treasure hunt adventure")
    assert r.branch == "Genre Fiction"
    assert r.leaf == "Treasure Hunt"


def test_classify_fiction_genre_multi_leaf_prunes_generic() -> None:
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("High Fantasy dragons", multi_leaf=True)
    assert r.branch == "Fantasy"
    # We should see specific leaves, not just the umbrella.
    assert "High Fantasy" in r.leaves
    assert "Dragon Fantasy" in r.leaves
    assert "Fantasy" not in r.leaves

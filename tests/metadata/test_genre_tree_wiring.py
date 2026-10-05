"""
Verify bundled genre trees, aliases and public lookup wiring.

The module keeps its fixtures and doubles local so the assertions remain
deterministic.

Example:
    Exercise test genre tree wiring through its owning regression module::

        python -m pytest -q tests/metadata/test_genre_tree_wiring.py
"""


def test_classify_fiction_genre_scifi_space_opera() -> None:
    """
    Verify classify fiction genre scifi space opera.

    Example:
        Exercise test classify fiction genre scifi space opera through its owning regression module::

            python -m pytest -q tests/metadata/test_genre_tree_wiring.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Space Opera")
    assert r.branch == "Science Fiction"
    assert r.leaf == "Space Opera"


def test_classify_fiction_genre_scifi_time_travel() -> None:
    """
    Verify classify fiction genre scifi time travel.

    Example:
        Exercise test classify fiction genre scifi time travel through its owning regression module::

            python -m pytest -q tests/metadata/test_genre_tree_wiring.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("sci fi / time travel")
    assert r.branch == "Science Fiction"
    assert r.leaf == "Time Travel"


def test_classify_fiction_genre_fantasy_urban() -> None:
    """
    Verify classify fiction genre fantasy urban.

    Example:
        Exercise test classify fiction genre fantasy urban through its owning regression module::

            python -m pytest -q tests/metadata/test_genre_tree_wiring.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Urban fantasy")
    assert r.branch == "Fantasy"
    assert r.leaf == "Urban Fantasy"


def test_classify_fiction_genre_romance_romantasy() -> None:
    """
    Verify classify fiction genre romance romantasy.

    Example:
        Exercise test classify fiction genre romance romantasy through its owning regression module::

            python -m pytest -q tests/metadata/test_genre_tree_wiring.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Romantasy")
    assert r.branch == "Romance"
    assert r.leaf == "Fantasy Romance"


def test_classify_fiction_genre_mystery_cozy() -> None:
    """
    Verify classify fiction genre mystery cozy.

    Example:
        Exercise test classify fiction genre mystery cozy through its owning regression module::

            python -m pytest -q tests/metadata/test_genre_tree_wiring.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Cozy Mystery")
    assert r.branch == "Mystery/Crime/Thriller"
    assert r.leaf == "Cozy Mystery"


def test_classify_fiction_genre_horror_gothic() -> None:
    """
    Verify classify fiction genre horror gothic.

    Example:
        Exercise test classify fiction genre horror gothic through its owning regression module::

            python -m pytest -q tests/metadata/test_genre_tree_wiring.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Gothic Horror")
    assert r.branch == "Horror"
    assert r.leaf == "Gothic Horror"


def test_classify_fiction_genre_literary_litfic() -> None:
    """
    Verify classify fiction genre literary litfic.

    Example:
        Exercise test classify fiction genre literary litfic through its owning regression module::

            python -m pytest -q tests/metadata/test_genre_tree_wiring.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("lit fic")
    assert r.branch == "Literary & General"
    assert r.leaf == "Literary Fiction"


def test_classify_fiction_genre_genre_treasure_hunt() -> None:
    """
    Verify classify fiction genre genre treasure hunt.

    Example:
        Exercise test classify fiction genre genre treasure hunt through its owning regression module::

            python -m pytest -q tests/metadata/test_genre_tree_wiring.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("Treasure hunt adventure")
    assert r.branch == "Genre Fiction"
    assert r.leaf == "Treasure Hunt"


def test_classify_fiction_genre_multi_leaf_prunes_generic() -> None:
    """
    Verify classify fiction genre multi leaf prunes generic.

    Example:
        Exercise test classify fiction genre multi leaf prunes generic through its owning regression module::

            python -m pytest -q tests/metadata/test_genre_tree_wiring.py


    :return: None; the function records state or raises through its assertions.
    """
    from LiuXin_alpha.metadata.standardization import classify_fiction_genre

    r = classify_fiction_genre("High Fantasy dragons", multi_leaf=True)
    assert r.branch == "Fantasy"
    # We should see specific leaves, not just the umbrella.
    assert "High Fantasy" in r.leaves
    assert "Dragon Fantasy" in r.leaves
    assert "Fantasy" not in r.leaves

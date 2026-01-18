
"""
Load the test data into the database, dump it to csv, and then reload it - tests the entire test_data cycle.
"""

if __name__ == "__main__":

    # Makes the test data and dumps it to file
    from LiuXin_tests.test_databases import make_test_data

    make_test_data()

    # Blank the database and then reload using the data that just got written
    from LiuXin_alpha.databases.database import Database

    Database(create=True)

    from LiuXin_tests.test_databases import load_data

    load_data(overwrite_db=True)

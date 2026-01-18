
"""
This does not, in fact, work.
"""


import os

from LiuXin_alpha.constants.paths import LiuXin_base_folder

if __name__ == "__main__":

    test_covers_folder = os.path.join(LiuXin_base_folder, "LiuXin_data", "test_covers")

    for i in range(1, 100):

        test_cover_name = "book_id_{}.jpg".format(i)
        test_cover_path = os.path.join(test_covers_folder, test_cover_name)

        if not os.path.exists(test_cover_path):
            with open(test_cover_path, "w+") as tf:
                tf.write("This is not a valid JPEG. Just a random test file with the right name.")

        pass

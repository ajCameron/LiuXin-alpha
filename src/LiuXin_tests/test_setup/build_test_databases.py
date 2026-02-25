
"""
Constructs all the test databases.
"""

import argparse
import imp
import os
import re
import traceback
import shutil
import time
from functools import partial

from clint.textui import puts, colored

from LiuXin_alpha.constants.paths import LiuXin_data_folder

from LiuXin_tests.test_databases import test_data_folder
from LiuXin_tests.test_fsms import TestFSMBuilderFromExistingDB
from LiuXin_tests.test_setup.constants import test_asset_version

from LiuXin_alpha.utils.file_ops.file_ops import ensure_folder
from LiuXin_alpha.utils.file_ops.file_ops import get_folders
from LiuXin_alpha.utils.file_ops.file_ops import get_files


def build_all_test_databases(
    dump=False,
    only_build_new=False,
    specific_build=None,
    fsm_build=True,
    parallel=False,
):
    """
    Constructs all specified test databases - test database are built from submodules in the test_databases modules -
    each test database should present a build_self method at the root of the module which can be used to construct the
    test database.
    Unless an override is provided test database will be copied into the test_databases folder in LiuXin_data once they
    have been built.
    :param dump: If True then the test_db files will be written out in the form of csv files.
                 These files will be written into the corresponding db folder in the test_data folder in test.
    :type dump: bool

    :param only_build_new: If True, then the data folder containing the output databases will be examined.
                           Only test databases not already in the test_databases folder will be built.
    :type only_build_new: bool

    :param specific_build: Allows you to specify a single database to build
    :type specific_build: str

    :param fsm_build: Do you also want the test database to have a folder store manager (containing a single empty
                        folder) built for them as well? This will be stored with the rest in the test_fsms folder

    :param parallel: If False, then the build will be run with a single process
                     If an integer, then the build will be run with that number of processes

    :return:
    """
    # Ensure the existence of the test database folder
    test_db_dir = os.path.join(LiuXin_data_folder, "test_databases")
    ensure_folder(test_db_dir)

    if fsm_build:
        test_fsm_dir = os.path.join(LiuXin_data_folder, "test_fsms")
        ensure_folder(test_fsm_dir)
    else:
        test_fsm_dir = None

    test_data_packages = get_data_packages(
        test_db_dir=test_db_dir,
        only_build_new=only_build_new,
        specific_build=specific_build,
    )

    # For each of the found databases run it's build_test_db method
    if not parallel:
        for test_db_name in test_data_packages:
            do_test_db_build(
                test_db_dir=test_db_dir,
                test_fsm_dir=test_fsm_dir,
                test_db_name=test_db_name,
                fsm_build=fsm_build,
                local_test_asset_version=test_asset_version,
            )
    else:
        from multiprocessing import Pool

        do_test_db_build_limited = partial(
            do_test_db_build,
            test_db_dir=test_db_dir,
            test_fsm_dir=test_fsm_dir,
            fsm_build=fsm_build,
            local_test_asset_version=test_asset_version,
        )
        p = Pool(processes=parallel)
        p.map(func=do_test_db_build_limited, iterable=test_data_packages)
        p.join()


def do_test_db_build(
    test_db_name,
    test_db_dir,
    test_fsm_dir,
    fsm_build=True,
    suppress_exceptions=False,
    local_test_asset_version=None,
):
    """
    Do the actual work of building a db.
    :param test_db_dir:
    :param test_fsm_dir:
    :param test_db_name:
    :param fsm_build: If True, then the database will have a blank fsm built for it
    :param suppress_exceptions: If True then exceptions will not halt the program - instead the status dict will be
                                returned

    :return status_dict: The results of running the build
    """
    status_dict = {
        "build_complete": False,
        "error_msg": [],
        "build_start": str(time.time()),
        "build_end": None,
    }

    db_package = imp.load_package(test_db_name, os.path.join(test_data_folder, test_db_name))

    if not hasattr(db_package, "build_test_db"):
        build_err_str = "{} has no build_test_db package - ignored for this build".format(test_db_name)
        puts(colored.red(build_err_str))

        # Record and return telemetry on the failed build attempt
        status_dict["error_msg"].append(build_err_str)

        return status_dict

    # Build the final destination name and path for the test database
    dst_db_name = "{}.test_db".format(test_db_name)
    dst_file_path = os.path.join(test_db_dir, dst_db_name)

    # Construct the package - method will take care of loading the database into the test db folder
    try:
        db_package.build_test_db(
            dst_file_path=dst_file_path,
            dump=False,
            plugin_name=test_db_name,
            test_asset_version=local_test_asset_version,
        )
    except Exception as e:
        err_msg = ["Error while trying to build the test database {}".format(test_db_name)]
        puts(colored.red("\n".join(err_msg)))

        if suppress_exceptions:

            status_dict["error_msg"] += err_msg
            status_dict["error_msg"] += [
                "type(exception): {}".format(type(e)),
                "e.message: {}".format(e.message),
                "e.args: {}".format(e.args),
            ]
            status_dict["error_msg"] += ["traceback: \n{}\n".format(traceback.format_exc())]

            status_dict["build_end"] = str(time.time())

            return status_dict
        else:
            raise

    # If this option is True then also build a test fsm around the database
    if fsm_build:
        dst_fsm_db_name = "{}_fsm".format(test_db_name)

        # Build the final destination path for the test folder store manager - remove it if it already exists
        dst_fsm_path = os.path.join(test_fsm_dir, dst_fsm_db_name)
        if os.path.exists(dst_fsm_path):
            shutil.rmtree(dst_fsm_path)

        db_fsm_builder = TestFSMBuilderFromExistingDB(
            dst_dir_path=dst_fsm_path,
            dump=False,
            fs_count=1,
            test_fsm_name=dst_fsm_db_name,
            override_db_path=dst_file_path,
        )
        try:
            db_fsm_builder.build()
        except Exception as e:
            err_msg = ["Error while trying to build fsm for the test database {}".format(test_db_name)]
            puts(colored.red("\n".join(err_msg)))

            if suppress_exceptions:
                status_dict["error_msg"] += err_msg
                status_dict["error_msg"] += [
                    "type(exception): {}".format(type(e)),
                    "e.message: {}".format(e.message),
                    "e.args: {}".format(e.args),
                ]
                status_dict["error_msg"] += ["traceback: \n{}\n".format(traceback.format_exc())]

                status_dict["build_end"] = str(time.time())

                return status_dict
            else:
                raise

    status_dict["build_complete"] = True
    status_dict["build_end"] = str(time.time())
    return status_dict


def get_data_packages(test_db_dir=None, only_build_new=False, specific_build=None):

    # Introspect to find the test databases to load
    keep_re = r"test_db_[0-9]+"
    test_data_packages = sorted([mn for mn in get_folders(test_data_folder) if re.match(keep_re, mn)])

    if only_build_new:

        assert test_db_dir is not None, "Need a dir to compare against the existing database"

        built_test_db_re = r"(test_db_[0-9]+).test_db"
        built_test_dbs = sorted([dbfn for dbfn in get_files(test_db_dir) if re.match(built_test_db_re, dbfn)])

        built_test_dbs_package_names = set(
            [re.match(built_test_db_re, fn).group(1) for fn in built_test_dbs if re.match(built_test_db_re, fn)]
        )

        # Filter out the databases which have already been built
        test_data_packages = sorted([mn for mn in test_data_packages if mn not in built_test_dbs_package_names])

    if specific_build is not None:
        # Check that the specified build is a valid one
        assert (
            specific_build in test_data_packages
        ), 'specific_build "{}" not recognized in the valid packages list "{}"'.format(
            specific_build, test_data_packages
        )
        test_data_packages = [
            specific_build,
        ]

    # Todo: Use package_good_sort over in test fsms
    test_data_packages = package_good_sort(test_data_packages)

    return test_data_packages


def package_good_sort(package_name_list):
    """
    Preforms an actually helpful sorting of the package names.
    :param package_name_list:
    :return:
    """
    pos_name_map = dict()
    for package_name in package_name_list:

        package_pos = int(package_name.split("_")[-1])
        pos_name_map[package_pos] = package_name

    return [pos_name_map[np] for np in sorted(pos_name_map.keys())]


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Build Tests Databases")
    parser.add_argument(
        "--build_new",
        dest="only_build_new",
        action="store_const",
        const=True,
        default=False,
        help="If flag provided, will only build new test databases (test databases without files in "
        "LiuXin_data.test_databases)",
    )
    parser.add_argument(
        "--dump",
        dest="dump",
        action="store_const",
        const=True,
        default=False,
        help="If flag provided all databases built during this action will be dumped as CSV files "
        "into their folder in test_data",
    )
    parser.add_argument("--specific_build", dest="specific_build", help="Build a single test database")
    parser.add_argument(
        "--fsm_build",
        dest="fsm_build",
        action="store_const",
        const=True,
        default=True,
        help="Build test folder stores for all the database. Test folder store will contain a single, "
        "empty, folder store.",
    )
    parser.add_argument(
        "-p",
        "--parallel",
        dest="parallel",
        type=int,
        default=1,
        help="Run the database build process in parallel - pass a number of processes to run that many " "concurrently",
    )

    args = parser.parse_args()
    args_dir = vars(args)

    if args_dir["parallel"] == 1:
        args_dir["parallel"] = False

    build_all_test_databases(**args_dir)

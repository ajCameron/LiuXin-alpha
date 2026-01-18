# Constructs the test databases

import argparse
import imp
import os
import pprint
import re
import shutil
import time
import traceback
from functools import partial

from clint.textui import puts, colored

from LiuXin.paths import LiuXin_data_folder

from LiuXin_tests.test_fsms import test_data_folder

from LiuXin.utils.file_ops.file_ops import ensure_folder
from LiuXin.utils.file_ops.file_ops import get_folders
from LiuXin.utils.file_ops.file_ops import get_files
from LiuXin.utils.terminal import safe_terminal_info_print


# Todo: Check build in LiuXin_data/test_folder_store_managers - everything should point there
def build_all_test_folder_store_managers(dump=False, only_build_new=False, specific_build=None, parallel=False):
    """
    Constructs all specified test folder store managers - test database are built from submodules in the test_data
    modules - each test folder_store_manager should present a build_self method at the root of the module which can be
    used to construct the test database.
    Unless an override is provided test folder store managers will be copied into the test_fsms folder in LiuXin data.
    :param dump: If True then the generated database will be written out in the form of csvs.
                 A gist of the folder store will also be written out.
    :type dump: bool

    :param only_build_new: If True, then the data folder containing the output folder store managers will be examined.
                           Build will only be run on folder store managers which have a build recipe but no entry in
                           the test folder store managers folder.
    :type only_build_new: bool

    :param specific_build: Allows you to specify a single folder store manager to build
    :type specific_build: str

    :param parallel: Should the build process be run in parallel?

    :return:
    """
    run_start = time.time()

    # Ensure the existence of the test folder store managers folder
    test_fsm_dir = os.path.join(LiuXin_data_folder, "test_fsms")
    ensure_folder(test_fsm_dir)

    test_data_packages = get_data_packages(test_fsm_dir, only_build_new=only_build_new, specific_build=specific_build)

    # Todo: Spin this off into a function for easier error hamdling

    safe_terminal_info_print(
        [
            "Executing build for the following test fsms",
            pprint.pformat(test_data_packages),
        ]
    )

    # For each of the found folder store manager packages execute build for that folder store manager
    if not parallel:
        for test_fsm_name in test_data_packages:
            do_test_fsm_build(
                test_data_folder=test_data_folder,
                test_fsm_dir=test_fsm_dir,
                test_fsm_name=test_fsm_name,
                dump=dump,
            )
    else:
        from multiprocessing import Pool

        do_test_fsm_build_limited = partial(
            do_test_fsm_build,
            test_fsm_dir=test_fsm_dir,
            test_data_folder=test_data_folder,
            dump=dump,
            suppress_exceptions=True,
        )
        p = Pool(parallel)
        status_list = p.map(func=do_test_fsm_build_limited, iterable=test_data_packages)
        # Should not be needed - but the main process seems to be terminating before the workers
        p.join()

        print_status_list(test_data_packages=test_data_packages, status_list=status_list)

    run_end = time.time()

    safe_terminal_info_print(
        [
            "Build for ",
            pprint.pformat(test_data_packages),
            "completed in {} seconds".format(run_end - run_start),
        ]
    )


def do_test_fsm_build(test_fsm_name, test_data_folder, test_fsm_dir, dump, suppress_exceptions=False):
    """
    Does the actual work of building a specific test fsm.
    :param test_fsm_name: The name of the fsm to do the build for
    :param test_data_folder: Contains the data objects (test covers, md test files, e.t.c)
    :param test_fsm_dir: The folder containing the test fsm packages to build off
    :param dump: If True, then the resulting database will be written out in csv format - not currently supported
    :param suppress_exceptions: If True then exceptions will not be raised - just logged and telemetry returned

    :return:
    """
    status_dict = {
        "test_fsm_name": test_fsm_name,
        "build_complete": False,
        "error_msg": [],
        "build_start": str(time.time()),
        "build_end": None,
    }

    fsm_package = imp.load_package(test_fsm_name, os.path.join(test_data_folder, test_fsm_name))
    if hasattr(fsm_package, "build_test_folder_store_manager"):

        # Build the final destination path for the test folder store manager - remove it if it already exists
        dst_fsm_path = os.path.join(test_fsm_dir, test_fsm_name)
        if os.path.exists(dst_fsm_path):
            shutil.rmtree(dst_fsm_path)

        # Construct the package - method will take care of loading the database into the test db folder
        try:
            fsm_package.build_test_folder_store_manager(
                dst_dir_path=dst_fsm_path, dump=dump, test_fsm_name=test_fsm_name
            )
        except Exception as e:
            # Todo: Add more complete logging here
            err_msg = [
                "Error while trying to build - {}".format(test_fsm_name),
            ]
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

    else:

        puts(colored.red("{} has no build_test_db package - ignored for this build".format(test_fsm_name)))


def get_data_packages(test_fsm_dir=None, only_build_new=False, specific_build=None):

    # Introspect to find the test databases to load - modified with the parameters passed to this method
    keep_re = r"test_fsm_[0-9]+"
    test_data_packages = sorted([mn for mn in get_folders(test_data_folder) if re.match(keep_re, mn)])

    if only_build_new:

        assert test_fsm_dir is not None, "need a folder containing existing folder stores to compare against"

        # Read the output folder to find the already built folder store managers
        built_test_fsm_re = r"(test_fsm_[0-9]+)"
        built_test_fsms = sorted([dbdn for dbdn in get_files(test_fsm_dir) if re.match(built_test_fsm_re, dbdn)])

        built_test_fsms_package_names = set(
            [re.match(built_test_fsm_re, fn).group(1) for fn in built_test_fsms if re.match(built_test_fsm_re, fn)]
        )

        # Filter out the folder store managers which have already been built
        test_data_packages = sorted([mn for mn in test_data_packages if mn not in built_test_fsms_package_names])

    test_data_packages = sorted(test_data_packages, key=lambda x: int(x.split("_")[2]))

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

    return test_data_packages


def print_status_list(test_data_packages, status_list):
    """
    Print the results of building the test fsms.
    :param test_data_packages: A list of all the names of the packages to build.
    :param status_list: A list of all the status dicts from the build process
    """
    status_dict = dict((le["test_fsm_name"], le) for le in status_list)

    for package_name in test_data_packages:

        package_status = status_dict[package_name]

        build_end = int(package_status["build_end"])
        build_start = int(package_status["build_start"])

        puts(colored.white("-" * 20))
        if package_status["build_complete"]:
            puts(colored.green("Package {} built in {} seconds".format(package_name, build_end - build_start)))
        else:
            err_msg = ["Package {} build failed in {} seconds".format(package_name, build_end - build_start)]
            err_msg += package_status["error_msg"]
            puts(colored.red("\n".join(err_msg)))
    puts(colored.white("-" * 20))


if __name__ == "__main__":

    parser = argparse.ArgumentParser(description="Build Tests Folder Store Managers")
    parser.add_argument(
        "--build_new",
        dest="only_build_new",
        action="store_const",
        const=True,
        default=False,
        help="If flag provided, will only build new test folder store managers (test folder store "
        "managers without files in LiuXin_data.test_folder_store_managers",
    )
    parser.add_argument(
        "--dump",
        dest="dump",
        action="store_const",
        const=True,
        default=False,
        help="If flag provided gists of all folder store managers will be dumped into the folders "
        "containing the build script",
    )
    parser.add_argument("--specific_build", dest="specific_build", help="Build a single test database")
    parser.add_argument(
        "-p",
        "--parallel",
        dest="parallel",
        type=int,
        default=1,
        help="Run the fsm build process in parallel",
    )

    args = parser.parse_args()
    args_dir = vars(args)

    if args_dir["parallel"] == 1:
        args_dir["parallel"] = False

    build_all_test_folder_store_managers(**args_dir)

#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
Expose deterministic-compatible random helpers under the retained import path.

The module keeps compatibility policy, normalization and resource ownership explicit
for callers.

Example:
    Exercise liuxin random through a consuming regression::

        python -m pytest -q tests/scripts/test_docstring_migration.py
"""

from copy import deepcopy


# Todo: Update the internal methods so they all have the same signature as random
class LiuXinBadPseudoRandomGenerator:
    """
    It's quite a bad pseudo-random number generator, alright.

    Example:
        Exercise LiuXinBadPseudoRandomGenerator through a consuming regression::

            python -m pytest -q tests/scripts/test_docstring_migration.py
    """

    def __init__(self, seed):
        """
        Start up the rng.

        Example:
            Exercise LiuXinBadPseudoRandomGenerator.  init   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param seed: Value supplied for seed under the utility contract.
        :return: None; validated state is stored on the receiving object.
        """
        self.seed = seed

        # Create a length 624 list to store the state of the generator
        self.MT = [0 for i in range(624)]
        self.index = 0

        # To get last 32 bits
        self.bitmask_1 = (2**32) - 1

        # To get 32. bit
        self.bitmask_2 = 2**31

        # To get last 31 bits
        self.bitmask_3 = (2**31) - 1

        self._initialize_generator()

    def _initialize_generator(self):
        """
        Initialize the generator from a seed

        Example:
            Exercise LiuXinBadPseudoRandomGenerator. initialize generator through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        self.MT[0] = self.seed
        for i in range(1, 624):
            self.MT[i] = ((1812433253 * self.MT[i - 1]) ^ ((self.MT[i - 1] >> 30) + i)) & self.bitmask_1

    def extract_number(self):
        """
        Extract a tempered pseudorandom number based on the index-th value, calling generate_numbers() every 624 numbers

        Example:
            Exercise LiuXinBadPseudoRandomGenerator.extract number through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if self.index == 0:
            self.generate_numbers()
        y = self.MT[self.index]
        y ^= y >> 11
        y ^= (y << 7) & 2636928640
        y ^= (y << 15) & 4022730752
        y ^= y >> 18

        self.index = (self.index + 1) % 624
        return y

    def __enter__(self):
        """
        Store the state of the rng for restore on exit.

        Example:
            Exercise LiuXinBadPseudoRandomGenerator.  enter   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        self.old_MT = deepcopy(self.MT)
        self.old_index = deepcopy(self.index)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        """
        Implement the resource's exit lifecycle operation.

        Example:
            Exercise LiuXinBadPseudoRandomGenerator.  exit   through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param exc_type: Value supplied for exc type under the utility contract.
        :param exc_val: Value supplied for exc val under the utility contract.
        :param exc_tb: Value supplied for exc tb under the utility contract.
        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        assert exc_type is None, exc_type

        self.MT = deepcopy(self.old_MT)
        self.index = deepcopy(self.old_index)

    def generate_numbers(self):
        """
        Generate an array of 624 untempered numbers

        Example:
            Exercise LiuXinBadPseudoRandomGenerator.generate numbers through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :return: None; the operation mutates state, writes output or performs cleanup in
            place.
        """
        for i in range(624):
            y = (self.MT[i] & self.bitmask_2) + (self.MT[(i + 1) % 624] & self.bitmask_3)
            self.MT[i] = self.MT[(i + 397) % 624] ^ (y >> 1)
            if y % 2 != 0:
                self.MT[i] ^= 2567483615

    def choice(self, target_list):
        """
        Chose an element from a list

        Example:
            Exercise LiuXinBadPseudoRandomGenerator.choice through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param target_list: Value supplied for target list under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        list_len = len(target_list)
        pos = self.extract_number() % list_len
        return target_list[pos]

    def randint(self, start, end):
        """
        Get a random int from within the given range. Does include the start and the end numbers.

        Example:
            Exercise LiuXinBadPseudoRandomGenerator.randint through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param start: Value supplied for start under the utility contract.
        :param end: Value supplied for end under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        if start == end:
            return start

        try:
            return self.choice(range(start, end + 1))
        except ZeroDivisionError:
            raise ValueError("start: {} - end: {}".format(start, end))

    def randrange(self, start, end):
        """
        Does include the start but does not include the end.

        Example:
            Exercise LiuXinBadPseudoRandomGenerator.randrange through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :param start: Value supplied for start under the utility contract.
        :param end: Value supplied for end under the utility contract.
        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.choice(range(start, end))

    def random(self):
        """
        Return a random integer.

        Example:
            Exercise LiuXinBadPseudoRandomGenerator.random through a consuming regression::

                python -m pytest -q tests/scripts/test_docstring_migration.py


        :return: The normalized value, metadata record, path, stream result or collection
            described above.
        """
        return self.extract_number()

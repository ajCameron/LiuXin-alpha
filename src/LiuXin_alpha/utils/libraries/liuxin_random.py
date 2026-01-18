#!/usr/bin/env python2
# -*- coding: utf-8 -*-

"""
Based on the pseudocode in https://en.wikipedia.org/wiki/Mersenne_Twister. Generates uniformly distributed
32-bit integers in the range [0, 232 − 1] with the MT19937 algorithm

Yaşar Arabacı <yasar11732 et gmail nokta com>
"""

from copy import deepcopy


# Todo: Update the internal methods so they all have the same signature as random
class LiuXinBadPseudoRandomGenerator:
    """
    It's quite a bad pseudo-random number generator, alright.

    But I need a convenient rng with some additional methods, and random is producing different results between
    platforms (I suspect it's the mix of setting's I'm using(.
    Rather than using a psuedo-black box, creating my own (very bad) rng.
    """

    def __init__(self, seed):
        """
        Start up the rng.
        :param seed:
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
        :param seed:
        :return:
        """
        self.MT[0] = self.seed
        for i in range(1, 624):
            self.MT[i] = ((1812433253 * self.MT[i - 1]) ^ ((self.MT[i - 1] >> 30) + i)) & self.bitmask_1

    def extract_number(self):
        """
        Extract a tempered pseudorandom number based on the index-th value,
        calling generate_numbers() every 624 numbers
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
        :return:
        """
        self.old_MT = deepcopy(self.MT)
        self.old_index = deepcopy(self.index)
        return self

    def __exit__(self, exc_type, exc_val, exc_tb):
        assert exc_type is None, exc_type

        self.MT = deepcopy(self.old_MT)
        self.index = deepcopy(self.old_index)

    def generate_numbers(self):
        """
        Generate an array of 624 untempered numbers
        :return:
        """
        for i in range(624):
            y = (self.MT[i] & self.bitmask_2) + (self.MT[(i + 1) % 624] & self.bitmask_3)
            self.MT[i] = self.MT[(i + 397) % 624] ^ (y >> 1)
            if y % 2 != 0:
                self.MT[i] ^= 2567483615

    def choice(self, target_list):
        """
        Chose an element from a list
        :param target_list:
        :return:
        """
        list_len = len(target_list)
        pos = self.extract_number() % list_len
        return target_list[pos]

    def randint(self, start, end):
        """
        Get a random int from within the given range.
        Does include the start and the end numbers.
        :param start:
        :param end:
        :return:
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
        :param start:
        :param end:
        :return:
        """
        return self.choice(range(start, end))

    def random(self):
        """
        Return a random integer.
        :return:
        """
        return self.extract_number()

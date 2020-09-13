import argparse
import unittest

from utils.common import str_to_bool, stem_law_name


class TestCommon(unittest.TestCase):
    def test_str_to_bool(self):
        self.assertTrue(str_to_bool("YES"))
        self.assertTrue(str_to_bool("true"))
        self.assertFalse(str_to_bool("No"))
        self.assertTrue(str_to_bool(True))
        with self.assertRaises(argparse.ArgumentTypeError):
            str_to_bool("hell!")

    def test_stem_law_name(self):
        print(stem_law_name("fuenften buch sozialgesetzbuch"))

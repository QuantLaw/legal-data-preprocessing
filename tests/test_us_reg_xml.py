import unittest

from statutes_pipeline_steps.us_reg_to_xml import split_double_units


class MyTestCase(unittest.TestCase):
    def test_split_double_units(self):
        self.assertEqual(
            [["(a)"], ["(1) sdf"], ["(2) asdasd"], [["x", "y"]]],
            list(split_double_units([["(a)(1) sdf"], ["(a)(2) asdasd"], [["x", "y"]]])),
        )

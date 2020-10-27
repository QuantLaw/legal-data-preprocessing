import unittest

from bs4 import BeautifulSoup

from statutes_pipeline_steps.us_reference_areas import find_references, usc_pattern
from statutes_pipeline_steps.us_reference_parse import parse_references


class UsReferenceAreasTestCase(unittest.TestCase):
    @unittest.skip("TODO: Check if pattern is common")
    def test_double_usc(self):
        soup = BeautifulSoup(
            "<text>f 1986 (31 U.S.C. 3801-U.S.C. 3831) which</text>", "lxml-xml"
        )
        find_references(soup, usc_pattern, {"pattern": "block"})
        self.assertEqual(
            "<text>"
            "f 1986 ("
            '<reference pattern="block">31 U.S.C. 3801-U.S.C. 3831</reference>'
            ") which"
            "</text>",
            str(soup.find("text")),
        )

    def test_double_cfr_no_space(self):
        soup = BeautifulSoup(
            '<text>sdf<reference pattern="block">10CFR 70.61(b), (c), or (d)'
            "</reference>ssdf</text>",
            "lxml-xml",
        )
        parse_references(soup, 123, 456)
        self.assertEqual(
            '<text>sdf<reference parsed=\'[["cfr10", "70.61"]]\' pattern="block">'
            "10CFR 70.61(b), (c), or (d)</reference>ssdf</text>",
            str(soup.find("text")),
        )

    def test_double_cfr_no_space2(self):
        soup = BeautifulSoup(
            '<text>sdf<reference pattern="block">24 CFR5.105(c)</reference>ssdf</text>',
            "lxml-xml",
        )
        parse_references(soup, 123, 456)
        self.assertEqual(
            '<text>sdf<reference parsed=\'[["cfr24", "5.105"]]\' pattern="block">'
            "24 CFR5.105(c)</reference>ssdf</text>",
            str(soup.find("text")),
        )

    @unittest.skip("TODO: Check if pattern is common")
    def test_double_cfr_no_space3(self):
        soup = BeautifulSoup(
            "<text>dance with 5 U.S.C. 552(a) and 1CFR part 51. These mater</text>",
            "lxml-xml",
        )
        find_references(soup, usc_pattern, {"pattern": "block"})
        self.assertEqual(
            None,
            str(soup.find("text")),
        )

import unittest

from bs4 import BeautifulSoup

from statutes_pipeline_steps.us_reference_areas import find_references, usc_pattern


class UsReferenceAreasTestCase(unittest.TestCase):
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

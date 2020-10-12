import unittest

from bs4 import BeautifulSoup
from quantlaw.de_extract.statutes_parse import StatutesParser

from statutes_pipeline_steps.de_reference_parse import parse_reference_content


class TestDeReferenceParse(unittest.TestCase):
    def test_parse_reference_content_lower_s(self):
        reference = BeautifulSoup(
            '<reference pattern="inline">'
            "<main>§ 6 Absatz 1 Nummer 2 Buchstabe r, s, t und v</main>"
            "</reference>",
            "lxml-xml",
        ).reference
        parser = StatutesParser({})
        parse_reference_content(reference, parser)
        self.assertEqual(
            '[["6", "1", "2", "r"], '
            '["6", "1", "2", "s"], '
            '["6", "1", "2", "t"], '
            '["6", "1", "2", "v"]]',
            reference.attrs["parsed"],
        )

    def test_parse_reference_content_upper_s_ignore(self):
        reference = BeautifulSoup(
            '<reference pattern="inline">'
            "<main>§ 6 Absatz 1 Nummer 2 Buchstabe r, s, t, S</main>"
            "</reference>",
            "lxml-xml",
        ).reference
        parser = StatutesParser({})
        parse_reference_content(reference, parser)
        self.assertEqual(
            '[["6", "1", "2", "r"], ' '["6", "1", "2", "s"], ' '["6", "1", "2", "t"]]',
            reference.attrs["parsed"],
        )

    def test_parse_reference_content_upper_s_for_Satz(self):
        reference = BeautifulSoup(
            '<reference pattern="inline">'
            "<main>§ 6 Absatz 1 Nummer 2 S 4, S 5</main>"
            "</reference>",
            "lxml-xml",
        ).reference
        parser = StatutesParser({})
        parse_reference_content(reference, parser)
        self.assertEqual(
            '[["6", "1", "2", "4"], ' '["6", "1", "2", "5"]]', reference.attrs["parsed"]
        )

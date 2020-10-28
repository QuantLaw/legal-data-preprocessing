import random
import re
import string
from unittest import TestCase

from utils.string_list_contains import StringContainsAlign


def get_random_string(length):
    letters = string.ascii_lowercase + " " * 8
    result_str = "".join(random.choice(letters) for i in range(length))
    return re.sub(r"\s+", " ", result_str)


class StringContainsAlignTestCase(TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        list_random_part = [
            get_random_string(random.randint(100, 1000)) for _ in range(100)
        ]
        list_random_part_1 = [
            get_random_string(random.randint(100, 1000)) for _ in range(100)
        ]
        list_random_part_2 = [
            get_random_string(random.randint(100, 1000)) for _ in range(100)
        ]

        cls.test_list_1 = (
            list_random_part * 10
            + list_random_part_1
            + list_random_part * 25
            + ["sdf sdf", "sdfsdf"]
        )
        cls.test_list_2 = (
            list_random_part_2 * 15
            + list_random_part_1
            + list_random_part_2 * 20
            + ["sdf sdf", "sdfsdf"]
        )

    def test_align(self):

        aligner = StringContainsAlign()
        aligner.text_list_0 = self.__class__.test_list_1
        aligner.text_list_1 = self.__class__.test_list_2

        aligner.create_index()

        res = aligner.run(reversed=True)

        self.assertEqual(102, len(res))

        aligner.min_text_length = 100
        res = aligner.run(reversed=True)

        self.assertTrue(0 < len(res) < 102)

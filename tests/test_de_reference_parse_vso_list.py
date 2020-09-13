import unittest

from statutes_pipeline_steps.de_reference_parse_vso_list import (
    remove_duplicate_references,
)


class MyTestCase(unittest.TestCase):
    def test_remove_duplicate_references(self):
        self.assertEqual(
            remove_duplicate_references(
                [["SGB-4", "28p", "8"], ["SGB-4", "28p", "8"], ["SGB-4", "28p", "7"]]
            ),
            [["SGB-4", "28p", "8"], ["SGB-4", "28p", "7"]],
        )

        self.assertEqual(
            remove_duplicate_references(
                [
                    [["Gesetz", "EnWiG"], ["§", "2"], ["Abs", "2"]],
                    [["Gesetz", "EnWiG"], ["§", "2"], ["Abs", "3"]],
                    [["Gesetz", "EnWiG"], ["§", "5"]],
                    [["Gesetz", "EnWiG"], ["§", "5"]],
                ]
            ),
            [
                [["Gesetz", "EnWiG"], ["§", "2"], ["Abs", "2"]],
                [["Gesetz", "EnWiG"], ["§", "2"], ["Abs", "3"]],
                [["Gesetz", "EnWiG"], ["§", "5"]],
            ],
        )


if __name__ == "__main__":
    unittest.main()

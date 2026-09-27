import unittest
from coverage import check

class CoverageTest(unittest.TestCase):
    def test_threshold(self):
        for text in ["", "mode: atomic", "mode: atomic\nx:1 95 1\nx:2 5 0", "mode: atomic\nx:1 99 1\ny:1 100 0"]:
            with self.assertRaises(ValueError): check(text)
        self.assertEqual(check("mode: atomic\nx:1 96 1\nx:2 4 0"), (96,100))
    def test_invalid(self):
        for text in ["mode: set", "mode: atomic\nx:1 -1 1", "mode: atomic\nx:1 1 -1", "mode: atomic\nx:1 1 1\nx:1 1 1"]:
            with self.assertRaises(ValueError): check(text)

if __name__ == "__main__": unittest.main()

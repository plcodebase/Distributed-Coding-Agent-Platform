"""Acceptance test for the intentionally broken calculator fixture."""

import unittest

from calculator import add


class CalculatorTest(unittest.TestCase):
    def test_adds_two_integers(self) -> None:
        self.assertEqual(add(2, 3), 5)


if __name__ == "__main__":
    unittest.main()

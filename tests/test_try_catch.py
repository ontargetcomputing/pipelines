import pytest
import sys
sys.path.append("src")
from pipelines.try_catch import try_catch


class TestTryCatch:
    def test_function_called(self):
        # SETUP
        expected = 1

        def func():
            return expected

        # EXECUTE
        value = try_catch({}, func)

        # ASSERT
        assert value == expected

    def test_exception_raised(self):
        # SETUP
        def func():
            raise Exception("some exception")

        # ASSERT
        with pytest.raises(Exception):
            try_catch({}, func)

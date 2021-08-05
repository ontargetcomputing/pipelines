import pytest
import sys
sys.path.append("src")
from pipelines.functions.format.json.convert_to_csv import run


@pytest.mark.functions
class TestConvertToCsv:
    def test_run(self):
        # SETUP
        json_obj = {
            "foo": "bar",
            "whiz": "bang"
        }

        expected_csv = "bar,bang\n"

        # EXECUTE
        received_return = run(json_obj)

        # ASSERT
        assert received_return == expected_csv

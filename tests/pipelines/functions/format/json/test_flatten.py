import pytest
import sys
sys.path.append("src")
from pipelines.functions.format.json.flatten import run


@pytest.mark.functions
class TestConvertFlatten:
    def test_run(self):
        # SETUP
        obj = {
            "outer": {
                "inner": {
                    "foo": {
                        "bar": "whizbang",
                        "super_inner": {
                            "three": {
                                "four": {
                                    "hello": "world"
                                }
                            }
                        }
                    },
                    "hello": "world"
                }
            }
        }

        expected = {
            "outer_inner_foo_bar": "whizbang",
            "outer_inner_foo_super_inner_three_four_hello": "world",
            "outer_inner_hello": "world"
        }

        # EXECUTE
        received_return = run(obj)

        # ASSERT
        assert received_return == expected

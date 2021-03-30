import pytest
import sys
sys.path.append("src")
from pipelines.json.json_interrogator import JsonInterrogator


@pytest.mark.json_interrogator
class TestJsonInterrogator:
    def test_path_exists_top_level(self):
        # SETUP
        json_obj = {
            "outer": {
                "inner": {
                    "foo": {
                        "bar": "whizbang"
                    },
                    "hello": "world"
                }
            }
        }

        # EXECUTE
        json_interrogator = JsonInterrogator()
        exists = json_interrogator.path_exists(json_obj, "outer")

        # ASSERT
        assert exists is True

    def test_path_exists_lower_level(self):
        # SETUP
        json_obj = {
            "outer": {
                "inner": {
                    "foo": {
                        "bar": "whizbang"
                    },
                    "hello": "world"
                }
            }
        }

        # EXECUTE
        json_interrogator = JsonInterrogator()
        exists = json_interrogator.path_exists(json_obj, "outer.inner.foo")

        # ASSERT
        assert exists is True

    def test_determine_root_key(self):
        # SETUP
        obj = {
            "foo": {
                "bar": "whiz",
                "bang": "wow"
            }
        }

        # EXECUTE
        json_interrogator = JsonInterrogator()

        # ASSERT
        assert json_interrogator.determine_root_node(obj) == "foo"

    def test_determine_root_key_raises_on_more_keys(self):
        # SETUP
        obj = {
            "foo": {
                "bar": "whiz",
                "bang": "wow"
            },
            "extra": {
                "bar": "whiz2"
            }
        }

        # EXECUTE
        json_interrogator = JsonInterrogator()

        # ASSERT
        with pytest.raises(Exception, match="There must be only on root level node in *"):
            json_interrogator.determine_root_node(obj)

import pytest
import sys
sys.path.append("src")
from pipelines.lang.string_to_map import StringToMap

string_to_map = StringToMap()


@pytest.mark.string_to_map
class TestStringToMap:
    def test_to_map(self):
        # SETUP
        str = "json:lw-ext-staging-agrian-json,default:lw-ext-staging-agrian-non-json"

        # EXECUTE
        new_map = string_to_map.to_map(str)

        # ASSERT
        assert "json" in new_map.keys()
        assert "default" in new_map.keys()
        assert new_map["json"] == "lw-ext-staging-agrian-json"
        assert new_map["default"] == "lw-ext-staging-agrian-non-json"

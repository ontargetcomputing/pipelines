import pytest
import sys
sys.path.append("src")
from pipelines.functions.format.json.convert_points import run


@pytest.mark.functions
class TestConvertPoints:
    def test_run_with_point(self):
        # SETUP
        json_obj = {
            "foo": "bar",
            "centroid": "POINT (119.8692741460109 46.69003107655638)"
        }

        expected = {
            "foo": "bar",
            "centroid_latitude": "46.69003107655638",
            "centroid_longitude": "119.8692741460109"
        }

        # EXECUTE
        received_return = run(json_obj)

        # ASSERT
        assert received_return == expected

    def test_run_without_point(self):
        # SETUP
        json_obj = {
            "foo": "bar",
            "centroid": "APOINT (119.8692741460109 46.69003107655638)"
        }

        # EXECUTE
        received_return = run(json_obj)

        # ASSERT
        assert received_return == json_obj

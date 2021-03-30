import pytest
import sys
sys.path.append("src")
from pipelines.json.json_mutator import JsonMutator


@pytest.mark.json_mutator
class TestJsonMutator:
    def test_insert_value_to_path(self):
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

        path = "outer.inner.foo.newbar"
        value = "new value"

        # EXECUTE
        json_mutator = JsonMutator(json_obj)
        json_mutator.insert_value_to_path(path, value)
        updated_json_obj = json_mutator.json()

        # ASSERT
        assert updated_json_obj['outer']['inner']['foo']['newbar'] == value

    def test_flatten(self):
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

        # EXECUTE
        json_mutator = JsonMutator(obj)
        json_mutator.flatten()
        flattened = json_mutator.json()

        # ASSERT
        expected = {
            "outer_inner_foo_bar": "whizbang",
            "outer_inner_foo_super_inner_three_four_hello": "world",
            "outer_inner_hello": "world"
        }

        # ASSERT
        assert flattened == expected

    def test_replace_points_with_lat_long_without_comma(self):
        # SETUP
        obj = {
            "foo": {
                "bar": "whiz",
                "bang": "wow",
                "centroid2": "POINT (222.8692741460109 33.69003107655638)"
            },
            "extra": {
                "bar": "whiz2",
                "centroid": "POINT (119.8692741460109 46.69003107655638)"
            }
        }

        expected = {
            "foo": {
                "bar": "whiz",
                "bang": "wow",
                "centroid2_latitude": "222.8692741460109",
                "centroid2_longitude": "33.69003107655638"
            },
            "extra": {
                "bar": "whiz2",
                "centroid_latitude": "119.8692741460109",
                "centroid_longitude": "46.69003107655638"
            }
        }
        # EXECUTE
        json_mutator = JsonMutator(obj)
        json_mutator.replace_points_with_lat_long()
        updated_obj = json_mutator.json()

        # ASSERT
        assert updated_obj == expected

    def test_replace_points_with_lat_long_with_comma(self):
        # SETUP
        obj = {
            "foo": {
                "bar": "whiz",
                "bang": "wow",
                "centroid2": "POINT (222.8692741460109 33.69003107655638)",
                "one": "more"
            },
            "extra": {
                "bar": "whiz2",
                "centroid": "POINT (119.8692741460109 46.69003107655638)",
            }
        }

        expected = {
            "foo": {
                "bar": "whiz",
                "bang": "wow",
                "centroid2_latitude": "222.8692741460109",
                "centroid2_longitude": "33.69003107655638",
                "one": "more"
            },
            "extra": {
                "bar": "whiz2",
                "centroid_latitude": "119.8692741460109",
                "centroid_longitude": "46.69003107655638",
            }
        }
        # EXECUTE
        json_mutator = JsonMutator(obj)
        json_mutator.replace_points_with_lat_long()
        updated_obj = json_mutator.json()

        # ASSERT
        assert updated_obj == expected

    def test_csv(self):
        # SETUP
        obj = {
            "bar": "whiz",
            "bang": "wow",
            "centroid2": "aaa",
            "one": "more"
        }

        # EXECUTE
        json_mutator = JsonMutator(obj)
        csv = json_mutator.csv()

        expected_csv = "bar,bang,centroid2,one\nwhiz,wow,aaa,more\n"

        # ASSERT
        assert csv == expected_csv

    def test_update_json(self):
        # SETUP
        obj = {
            "foo": {
                "bar": "whiz",
                "bang": "wow123"
            }
        }

        path = "foo.new_location"
        value = "new value"

        # EXECUTE
        json_mutator = JsonMutator(obj)
        json_mutator.update_value(path, value)
        updated_json = json_mutator.json()

        # ASSERT
        assert updated_json['foo']['new_location'] == value

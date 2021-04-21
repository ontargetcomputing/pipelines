from flatten_json import flatten
import json
import logging
import pandas as pd
import re
from .json_interrogator import JsonInterrogator

json_interrogator = JsonInterrogator()


class JsonMutator:
    json_obj = None

    def __init__(self, json_obj):
        self.json_obj = json_obj

    # TODO: This is not super efficient, optimize in the future
    def insert_value_to_path(self, path, value):
        key_list = path.split(".")
        num_keys = len(key_list)

        current_dict = self.json_obj
        last_key = key_list.pop(num_keys - 1)
        if num_keys > 1:
            for key in key_list:
                if json_interrogator.path_exists(current_dict, key):
                    current_dict = current_dict[key]
                else:
                    current_dict[key] = {}
                    current_dict = current_dict[key]

        current_dict[last_key] = value

    def flatten(self):
        self.json_obj = flatten(self.json_obj, "_")

    def replace_points_with_lat_long(self):
        json_string = json.dumps(self.json_obj)
        matches = re.finditer(
            r'\"(\w+)\":\s+\"POINT \(([-+]?[0-9]*\.?[0-9]*) ([-+]?[0-9]*\.?[0-9]*)\)\"(,*)', json_string)
        match = next(matches, None)
        if match is not None:
            while match is not None:
                logging.debug(f'Match {match.group(0)}')
                replacement = f'"{match.group(1)}_latitude": "{match.group(2)}",\n"{match.group(1)}_longitude": "{match.group(3)}"{match.group(4)}'
                json_string = json_string.replace(match.group(0), replacement)
                match = next(matches, None)

            self.json_obj = json.loads(json_string)

    def update_value(self, path, value):
        nodes = path.split(".")
        node_key = nodes.pop()
        current_dict = self.json_obj
        for node in nodes:
            current_dict = current_dict[node]

        current_dict[node_key] = value

    def csv(self):
        df = pd.DataFrame(list(self.json_obj.values())).T
        return df.to_csv(index=False, header=False)

    def json(self):
        return self.json_obj

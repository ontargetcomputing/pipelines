import sys
import logging
sys.path.append("src")
from pipelines.format.json.json_mutator import JsonMutator


def run(json_obj):
    json_mutator = JsonMutator(json_obj)
    json_mutator.replace_points_with_lat_long()
    updated_json_obj = json_mutator.json()

    if json_obj == updated_json_obj:
        logging.debug('No POINT found in json')
    else:
        logging.debug('POINT found in json')

    return updated_json_obj

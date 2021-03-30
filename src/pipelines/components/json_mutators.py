import sys
sys.path.append("src")
from pipelines.json.json_mutator import JsonMutator


def convert_points(json_obj):
    json_mutator = JsonMutator(json_obj)
    json_mutator.replace_points_with_lat_long()
    updated_json_obj = json_mutator.json()

    if json_obj == updated_json_obj:
        print('No POINT found in json')
    else:
        print('POINT found in json')

    return updated_json_obj


def convert_to_csv(json_obj):
    json_mutator = JsonMutator(json_obj)
    return json_mutator.csv()


def flatten(json_obj):
    json_mutator = JsonMutator(json_obj)
    json_mutator.flatten()
    return json_mutator.json()

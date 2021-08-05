import sys
sys.path.append("src")
from pipelines.format.json.json_mutator import JsonMutator


def run(json_obj):
    json_mutator = JsonMutator(json_obj)
    return json_mutator.csv()

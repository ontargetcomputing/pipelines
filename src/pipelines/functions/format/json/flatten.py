import sys
sys.path.append("src")
from pipelines.format.json.json_mutator import JsonMutator


def run(json_obj):
    json_mutator = JsonMutator(json_obj)
    json_mutator.flatten()
    return json_mutator.json()

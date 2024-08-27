import json
import os

def get_path_script():
    return os.path.dirname(os.path.abspath(__file__))


def get_bannds() -> dict:
    spectrums_file = os.path.join(get_path_script(), "bands.json")

    with open(spectrums_file, "r") as json_file:
        spectrums = json.load(json_file)
    return spectrums




def get_classindex() -> dict:

    classindex_file = os.path.join(get_path_script(), "classindex.json")

    with open(classindex_file, "r") as json_file:
        classindex = json.load(json_file)
    return classindex

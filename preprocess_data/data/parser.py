import json
import os

def get_path_script():
    return os.path.dirname(os.path.abspath(__file__))
def get_stopwords():
    stopwords_file = os.path.join(get_path_script(), "stopwords.json")

    with open(stopwords_file, "r") as json_file:
        stopwords = json.load(json_file)
    return stopwords

def get_spectrums():
    spectrums_file = os.path.join(get_path_script(), "spectrums.json")

    with open(spectrums_file, "r") as json_file:
        spectrums = json.load(json_file)
    return spectrums

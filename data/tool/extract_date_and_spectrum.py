import pandas as pd

from .find_date_and_spectrum import find_date_and_spectrum

def extract_date_and_spectrum(df : pd.DataFrame) -> dict:
    
    info = list(df.columns)
    stopwords =  [
        "CLASS",
        "Name",
        "DESCRIPTION", 
        "Id",
        "RAND", 
        "gridcode", 
        "X", 
        "Y"
    ]
    info = [_info for _info in info if _info not in stopwords]
    date_and_spectrum = dict()
    info = list(map(find_date_and_spectrum, info))
        
    for _info in info:
        day, spectrum = int(_info[0]) , _info[1] 
        if day in date_and_spectrum:
            date_and_spectrum[day].add(spectrum)
        elif day not in date_and_spectrum:
            date_and_spectrum[day] = set()
            date_and_spectrum[day].add(spectrum)  
                
    date_and_spectrum = dict(sorted(date_and_spectrum.items(), key=lambda item : item[0]))
    return date_and_spectrum
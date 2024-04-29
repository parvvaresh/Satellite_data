def get_all_spectrum(date_and_spectrum : dict) -> list:
    
    all_spectrum  = list()
    for _, spectrum in date_and_spectrum.items():
        all_spectrum.extend(list(spectrum))
    return list(set(all_spectrum))
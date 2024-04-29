from get_all_spectrum import get_all_spectrum

def save_date_and_spectrum(date_and_spectrum : dict,
                           path_destination : str = "None") -> None:
    if path_destination == "None":
        file = open("info.txt", "w")

    else :
        path = path_destination + "/info.txt"
        file = open(path, "w")
        
    for day, spectrum in date_and_spectrum.items():
        spectrums = ""
        for temp  in spectrum:
            spectrums += temp + "-" 
        string = f"day : {day} - spectrums : {spectrums}"
        file.write(string + "\n")
        
    
    all_spectrum = get_all_spectrum(date_and_spectrum)
    string = f"number of unique spectrums are :‌ {len(all_spectrum)}"
    file.write(string)
    
    file.close()
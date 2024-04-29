def find_date_and_spectrum(text : str) -> list:
    index = None
    for index_temp, char in enumerate(text):
        if char == "_":
            index = index_temp
            break
            
    return [
        text[ : index],
        text[index + 1 : ]
    ]                           
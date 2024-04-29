def get_all_day(date_and_spectrum : dict) -> list:
    
    all_day  = list()
    for day, _ in date_and_spectrum.items():
        all_day.append(day)
    return all_day
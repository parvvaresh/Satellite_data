from datetime import datetime, timedelta

def fix_date(start_date : datetime, 
                  step : int,
                  iter : int) -> list:
    dates = []
    current_date = start_date
    for _ in range(iter):
        dates.append(current_date.strftime("%Y-%m-%d"))
        current_date += timedelta(days=step)
        
    return dates
from datetime import datetime


def is_valid_date(date_string):
    try:
        datetime.strptime(date_string, "%m-%d-%Y")
        return True
    except ValueError:
        return False

from datetime import datetime, timedelta


def gen_date_range(start: datetime, end: datetime, step: int=None):
    if not step:
        step = 1
    cur_date = start.date()
    end_date = end.date()
    result = []
    delta = timedelta(days=step)
    while cur_date <= end_date:
        result.append(cur_date.strftime("%Y-%m-%d"))
        cur_date += delta
    return result

def read_file(file_path, mode = 'r'):
    with open(file_path, mode = mode) as f:
       content = f.read()
    return content
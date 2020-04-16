import re
from datetime import datetime, timedelta, date
import json


ISO_FORMAT = "%Y-%m-%dT%H:%M:%S%z"
DATE_FORMAT = "%Y-%m-%d"
dur_regex = re.compile(r'^((?P<days>[\.\d]+?)d)?((?P<hours>[\.\d]+?)h)?((?P<minutes>[\.\d]+?)m)?((?P<seconds>[\.\d]+?)s)?$')


def parse_duration(time_str):
    parts = dur_regex.match(time_str)
    assert parts is not None, "Could not parse any time information from '{}'.  Examples of valid strings: '8h', '2d8h5m20s', '2m4s'".format(time_str)
    time_params = {name: float(param) for name, param in parts.groupdict().items() if param}
    return timedelta(**time_params)


def parse_date(date_str):
    strlen = len(date_str)
    if strlen == 10:
        return datetime.strptime(date_str, DATE_FORMAT)
    elif strlen > 10:
        return datetime.strptime(date_str[:10], DATE_FORMAT)
    else:
        raise ValueError("unknown date format")


def ts2path(date_str):
    dte = parse_datetime(date_str[:-5] + "+0000")
    return dte.strftime("%Y/%m/%d/%H-%M-%S")


def parse_datetime(date_str):
    strlen = len(date_str)
    if strlen == 10:
        return datetime.strptime(date_str, DATE_FORMAT)
    elif strlen == 24:
        return datetime.strptime(date_str, ISO_FORMAT)
    else:
        raise ValueError("unknown date format")


def push_xcom(data):
    with open('/airflow/xcom/return.json', 'w') as xcom:
        xcom.write(json_dumps(data))
        xcom.flush()


def now():
    return datetime.utcnow().strftime(ISO_FORMAT)


def decode(val):
    if val and hasattr(val, "decode"):
        return val.decode("utf8")
    return val


class CustomJsonEncoder(json.JSONEncoder):

    def default(self, obj):
        if isinstance(obj, bytes):
            return obj.decode("utf8")
        if isinstance(obj, datetime):
            return obj.strftime(ISO_FORMAT)
        if isinstance(obj, date):
            return obj.strftime(DATE_FORMAT)
        if hasattr(obj, "to_dict"):
            to_json = getattr(obj, "to_dict")
            if callable(to_json):
                return to_json()
        if hasattr(obj, "value"):
            return obj.value
        return json.JSONEncoder.default(self, obj)


def json_dumps(data, pretty=False):
    if pretty:
        return json.dumps(data, cls=CustomJsonEncoder, indent=4, sort_keys=True)
    return json.dumps(data, cls=CustomJsonEncoder)

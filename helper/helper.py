from datetime import datetime
import pytz
import time

def datetime_range(start, end, delta):
    current = start
    while current < end:
        yield current
        current += delta

def convert_tz(originalTimeStamp, originalTimeZone, targetTimeZone):
    """
    Function converts unix-timestamp in s from
    originalTimeZone to targetTimeZone in ms
    """
    newTimeStamp = pytz.timezone(
        originalTimeZone).localize(
            datetime.fromtimestamp(
                originalTimeStamp)).astimezone(
                    pytz.timezone(targetTimeZone))
    return time.mktime(newTimeStamp.timetuple()) * 1000

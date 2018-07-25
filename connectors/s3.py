from data.productevent import productevents
from helper.helper import *
import pandas as pd

class S3Handler:
    def __init__(self):
        pass

    def get_event_df(self, product, unix_start_timestamp, unix_end_timestamp):

        events = [i for i in productevents
                  if self.get_event_condition(
                      i, unix_start_timestamp,
                      unix_end_timestamp)]

        product_list = []
        for e in events:
            for v in e['events']:
                code_color = v['data']['custom_attributes']['codeColor']
                if (product and
                   code_color == product):
                    product_list.append((e["timestamp_unixtime_ms"],
                                         code_color))
                else:
                    product_list.append((e["timestamp_unixtime_ms"],
                                         code_color))

        cols = ["timestamp_unixtime_ms", 'code_color']
        df_events = pd.DataFrame(product_list, columns=cols)

        df_events['timestamp_unixtime_ms'] = df_events['timestamp_unixtime_ms'] / 1000
        df_events['timestamp_unixtime_ms'] = df_events['timestamp_unixtime_ms'].apply(
            (lambda x: convert_tz(
                x, "Europe/London", "America/Sao_Paulo"))) 
        df_events['timestamp_unixtime_ms'] = \
            pd.to_datetime(df_events['timestamp_unixtime_ms'], unit='ms')

        return df_events

    def get_event_condition(self, i, unix_start_timestamp, unix_end_timestamp):
        return (i['timestamp_unixtime_ms'] >= unix_start_timestamp and
                i['timestamp_unixtime_ms'] <= unix_end_timestamp)

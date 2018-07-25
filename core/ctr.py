from datetime import datetime
from datetime import timedelta
import pandas as pd
from connectors.db import DataBaseHandler
from connectors.s3 import S3Handler
from helper.helper import *


class CTR_Calculater:
    def __init__(self, model):
        print("model init", model)

        # initialize and transform request parameters
        self.start_timestamp = model['startTimestamp']
        self.end_timestamp = model['endTimestamp']

        self.unix_start_timestamp = convert_tz(
            datetime.strptime(
                self.start_timestamp,
                '%Y-%m-%d %H:%M:%S').timestamp(),
            "America/Sao_Paulo", "Europe/London")
        self.unix_end_timestamp = convert_tz(
            datetime.strptime(
                self.end_timestamp,
                '%Y-%m-%d %H:%M:%S').timestamp(),
            "America/Sao_Paulo", "Europe/London")
        self.aggregation = model['aggregation']
        self.product = None
        if 'product' in model:
            self.product = model['product']
        self.platform = None
        if 'platform' in model:
            self.platform = model['platform']

        # initialize aggregation dataframe to purchase and events 
        self.df_aggregation = pd.DataFrame()
        self.df_aggregation['startTimestamp'] = \
            [dt for dt in datetime_range(
                datetime.strptime(
                    self.start_timestamp,
                    '%Y-%m-%d %H:%M:%S'),
                datetime.strptime(
                    self.end_timestamp,
                    '%Y-%m-%d %H:%M:%S'),
                timedelta(minutes=self.aggregation))]
        self.df_aggregation['startTimestamp_asof'] = \
            pd.to_datetime(self.df_aggregation['startTimestamp'])
        self.df_aggregation.index = \
            self.df_aggregation['startTimestamp_asof']

        # initialize order dataframe
        db = DataBaseHandler('connectors/orders.db')
        self.df_oders = db.get_purchase_df(
            self.start_timestamp, self.end_timestamp,
            self.product, self.platform)

        # initialize event dataframe
        s3_con = S3Handler()
        self.df_events = s3_con.get_event_df(
            self.product,
            self.unix_start_timestamp,
            self.unix_end_timestamp)

        # merge dataframe with aggregation dataframe
        self.df_event_itervals = self.merge_by_time_interval(
            self.df_events, self.df_aggregation,
            'timestamp_unixtime_ms', self.aggregation)
        self.df_oders_itervals = self.merge_by_time_interval(
            self.df_oders, self.df_aggregation,
            'order_date', self.aggregation)

    def calculate_ctr(self):
        a = self.df_event_itervals.startTimestamp.unique()
        b = self.df_oders_itervals.startTimestamp.unique()
        iterator = list(set(a) & set(b))

        result = []
        for agg_time in iterator:
            oder_filter = (self.df_oders_itervals.startTimestamp == agg_time)
            unique_products = self.df_oders_itervals[oder_filter].code_color.tolist()

            event_filter = (self.df_event_itervals.startTimestamp == agg_time)
            event_iterator = self.df_event_itervals[event_filter].groupby('code_color')
            for product_key, product_value in event_iterator:
                if product_key in unique_products:
                    p = self.df_oders_itervals[(self.df_oders_itervals.code_color == product_key)
                                             & (self.df_oders_itervals.startTimestamp == agg_time)]
                    for platform_key, platform_value in p.groupby('device_type'):
                        no_p = len(platform_value)
                        no_v = len(product_value)
                        ctr = no_p / no_v
                        result.append({
                            "startTimestamp": str(agg_time)[:10],
                            "platform": platform_key,
                            "product": product_key,
                            "ctr": ctr})
        return result

    def merge_by_time_interval(self, df, df_aggregation, data_col, aggregation):
        """
        Function performs an asof merge, as we match on “forward” search
        selects the first row in the aggregation dataframe whose ‘on’ key
        is greater than or equal to the left’s key.
        """

        if data_col in df.columns.values:
            df['startTimestamp_asof'] = pd.to_datetime(
                df[data_col])
        else:
            return df

        df = df.sort_values(by='startTimestamp_asof')
        df.index = df['startTimestamp_asof']
        tol = pd.Timedelta(aggregation, unit='m')

        return pd.merge_asof(left=df,
                             right=df_aggregation,
                             on='startTimestamp_asof',
                             direction='backward',
                             tolerance=tol)

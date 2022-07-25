import numpy as np
from Local.ContentManger import *
from Connection.AppProgramingInterfaceRTT import *
import datetime as dt

class DriverForecast():

    def __init__(self, DF_schedule_input, DF_store_forecast_input):
        self.DF_schedule = DF_schedule_input
        self.DF_store_forecast = DF_store_forecast_input
        self.APIConRTT = AppProgramingInterfaceRTT()
        self.LocalCon = ContentManger()

    def _ApplyClusterParameters(self, input_DF_store_forecast):
        DF_store_forecast = input_DF_store_forecast.copy()
        # DF_cluster_parameter = self.LocalCon.GetLocalClusterParameters()
        DF_cluster_parameter = self.APIConRTT.GetClusterParameter()
        DF_cluster_parameter["cluster_id"] = DF_cluster_parameter["cluster_id"].apply(int)

        DF_cluster_parameter = DF_cluster_parameter.drop(columns=['id'])
        DF_store_forecast = pd.merge(DF_store_forecast,
                                     DF_cluster_parameter[['cluster_id', 'driver_time_customer', 'customer_time', 'reallocation_time']],
                                     left_on='cluster_id',
                                     right_on='cluster_id',
                                     how='left')

        DF_store_forecast = DF_store_forecast.drop(columns=['cluster_id'])

        return DF_store_forecast

    def _ApplyStoreParameters(self, input_DF_store_forecast):
        DF_store_forecast = input_DF_store_forecast.copy()
        #DF_store_parameter = self.LocalCon.GetLocalStoreParameters()
        DF_store_parameter = self.APIConRTT.GetStoreParameter()
        DF_store_parameter["id"], DF_store_parameter["store_id"] = DF_store_parameter["id"].apply(int), DF_store_parameter["store_id"].apply(int)
        DF_store_parameter = DF_store_parameter.drop(columns=['id'])
        DF_store_forecast = pd.merge(DF_store_forecast,
                                     DF_store_parameter,
                                     left_on='store_id',
                                     right_on='store_id',
                                     how='left')

        return DF_store_forecast

    def _CalculateDriverForecast(self, input_DF_store_forecast):
        DF_driver_forecast = input_DF_store_forecast.copy()

        DF_driver_forecast = DF_driver_forecast[['schedule_id', 'store_id', 'week_day', 'hour', 'orders_requirement',
                                                'order_forecast_factor', 'out_door_time', 'driver_time_customer',
                                                'customer_time', 'reallocation_time', 'percent_single_volume',
                                                'percent_double_volume', 'percent_triple_volume']]

        max_relocation_time = 0
        #max_relocation_time = 0

        DF_driver_forecast['driver_time_single_order'] = DF_driver_forecast['out_door_time'] + \
                                                  DF_driver_forecast['driver_time_customer'] + \
                                                  DF_driver_forecast['reallocation_time'] + \
                                                  DF_driver_forecast['customer_time'] + max_relocation_time

        DF_driver_forecast['driver_time_double_order'] = DF_driver_forecast['driver_time_single_order'] + \
                                                  DF_driver_forecast['driver_time_customer'] + \
                                                  DF_driver_forecast['customer_time']

        DF_driver_forecast['driver_time_triple_order'] = DF_driver_forecast['driver_time_double_order'] + \
                                                  DF_driver_forecast['driver_time_customer'] + \
                                                  DF_driver_forecast['customer_time']

        #factor_to_order_time = 2.3 * 2.6
        factor_to_order_time = 0


        DF_driver_forecast['avg_driver_time_single_order'] = (DF_driver_forecast['driver_time_single_order'] - factor_to_order_time) * \
                                              DF_driver_forecast['percent_single_volume']
        DF_driver_forecast['avg_driver_time_double_order'] = (DF_driver_forecast['driver_time_double_order'] - factor_to_order_time) * \
                                              DF_driver_forecast['percent_double_volume']
        DF_driver_forecast['avg_driver_time_triple_order'] = (DF_driver_forecast['driver_time_triple_order'] - factor_to_order_time) * \
                                              DF_driver_forecast['percent_triple_volume']


        DF_driver_forecast['avg_driver_time_order'] = DF_driver_forecast['avg_driver_time_single_order'] + DF_driver_forecast['avg_driver_time_double_order'] + DF_driver_forecast['avg_driver_time_triple_order']

        DF_driver_forecast['avg_order_driver_time'] = 60 / (DF_driver_forecast['avg_driver_time_order'])

        DF_driver_forecast['avg_orders_trip'] = DF_driver_forecast['percent_single_volume'] + DF_driver_forecast['percent_double_volume']*2 + DF_driver_forecast['percent_triple_volume']*3

        DF_driver_forecast['workforce_requirement'] = DF_driver_forecast['orders_requirement'] / (DF_driver_forecast['avg_order_driver_time']*DF_driver_forecast['avg_orders_trip'])

        return DF_driver_forecast



    def _CalculateClusterForecast(self, input_DF_driver_forecast):
        DF_driver_forecast = input_DF_driver_forecast.copy()

        DF_cluster_forecast = DF_driver_forecast.groupby(['schedule_id', 'week_day', 'hour'],as_index=False, sort=False).\
            agg({'workforce_requirement':'sum'}).reset_index(drop=True)

        DF_cluster_forecast['workforce_requirement'] = DF_cluster_forecast['workforce_requirement'].apply(np.ceil).apply(int)
        DF_cluster_forecast['hour'] = pd.to_datetime(DF_cluster_forecast['hour'])

        DF_cluster_forecast = DF_cluster_forecast.sort_values(by=['schedule_id','week_day', 'hour']).reset_index(drop=True)
        DF_cluster_forecast['hour'] = DF_cluster_forecast['hour'].dt.strftime('%H:%M:%S')  #.str[10:19]

        return DF_cluster_forecast

    def CreateClusterForecast(self):
        DF_store_forecast = self.DF_store_forecast.copy()
        DF_store_forecast['cluster_id'] = self.DF_schedule['cluster_id'][0]

        DF_store_forecast = self._ApplyClusterParameters(input_DF_store_forecast=DF_store_forecast)
        DF_store_forecast = self._ApplyStoreParameters(input_DF_store_forecast=DF_store_forecast)
        DF_driver_forecast = self._CalculateDriverForecast(input_DF_store_forecast=DF_store_forecast)
        DF_cluster_forecast = self._CalculateClusterForecast(input_DF_driver_forecast=DF_driver_forecast)

        return  DF_driver_forecast, DF_cluster_forecast



from Connection.AppProgramingInterfaceRTT import *
from Local.ContentManger import *

class OrderForecast():
    def __init__(self, input_DF_schedule_week=None, input_DF_schedule=None):
        self.DF_schedule_week = input_DF_schedule_week
        self.DF_schedule = input_DF_schedule
        self.APIConRTT = AppProgramingInterfaceRTT()
        self.LocalCon = ContentManger()

    def _GetStoresForecast(self, input_cluster_id):
        cluster_id = input_cluster_id
        DF_cluster_store = self.APIConRTT.GetClusterStore()
        DF_stores = DF_cluster_store[DF_cluster_store['cluster_id']==cluster_id].reset_index(drop=True)

        # DF_stores_forecast = self.LocalCon.GetStoreForcast()
        # DF_stores_forecast = DF_stores_forecast[DF_stores_forecast['store_id'].isin(DF_stores['store_id'])].reset_index(drop=True)

        DF_stores_forecast = self.APIConRTT.GetStoreForecast(DF_stores)

        # fix sorting
        DF_stores_forecast["hour_number"] = DF_stores_forecast["hour"].str[0:2].apply(int)
        DF_stores_forecast = DF_stores_forecast.sort_values(by=["store_id", "week_day", "hour_number"]).reset_index(drop=True)
        DF_stores_forecast = DF_stores_forecast.drop(columns=["hour_number"])


        return DF_stores_forecast

    def _TrimStoreForecastHours(self, input_DF_store_forecast):
        DF_store_forecast = input_DF_store_forecast.copy()
        print(DF_store_forecast['week_day'][0])
        amount_week_days = 7 - (DF_store_forecast['week_day'][0]-1)
        amount_stores = DF_store_forecast['store_id'].drop_duplicates(keep='first').reset_index(drop=True).count()
        DF_store_id =  DF_store_forecast[['store_id']].drop_duplicates(keep='first').reset_index(drop=True)
        for i in range(amount_week_days):
            for j in range(amount_stores):
                store_id = DF_store_id['store_id'][j]

                DF_store_forecast_I = DF_store_forecast[(DF_store_forecast['week_day']==(i+1))&(DF_store_forecast['store_id']==store_id)].reset_index(drop=True)
                min_index_num = DF_store_forecast_I[DF_store_forecast_I['orders_requirement'] > 0].index.min()
                max_index_num = DF_store_forecast_I[DF_store_forecast_I['orders_requirement'] > 0].index.max()
                DF_store_forecast_I = DF_store_forecast_I[(DF_store_forecast_I.index >= min_index_num) & (DF_store_forecast_I.index <= max_index_num)]

                DF_store_forecast = DF_store_forecast[~((DF_store_forecast['week_day']==(i+1))&(DF_store_forecast['store_id']==store_id))].reset_index(drop=True)
                DF_store_forecast = DF_store_forecast.append(DF_store_forecast_I).reset_index(drop=True)

        return DF_store_forecast

    def CreateOrderForcast(self):
        DF_schedule = self.DF_schedule
        schedule_id = DF_schedule['schedule_id'][0]
        cluster_id = DF_schedule['cluster_id'][0]

        DF_store_forecast = self._GetStoresForecast(input_cluster_id = cluster_id)
        DF_store_forecast = DF_store_forecast.rename(columns={'orders_forecast':'orders_requirement'})

        DF_store_forecast['schedule_id'] = schedule_id
        DF_store_forecast = DF_store_forecast[['schedule_id', 'store_id', 'week_day','hour','orders_requirement']]
        DF_store_forecast = self._TrimStoreForecastHours(DF_store_forecast)

        return DF_store_forecast, DF_store_forecast






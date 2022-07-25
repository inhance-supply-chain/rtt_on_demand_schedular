import requests
import json
import pandas as pd
import numpy as np
from collections import OrderedDict
import time

class AppProgramingInterfaceRTT():

    def __init__(self):
        self.base_url = 'https://apiondemand.rtt.co.za'
        self.api_key = '?api_key=99e1004e-b743-49e6-8000-16384b9f568d'

    def ByteToDataframe(self, input_byte_object):
        my_json = input_byte_object.decode('utf8')
        if my_json[0:1] != "[":
            my_json = "["+my_json+"]"
        data = json.loads(my_json, object_pairs_hook=OrderedDict)
        DF_result = pd.DataFrame.from_dict(data)
        return DF_result

    def GetScheduleWeek(self, input_start_date):
        methode = "/api/OnDemand/GetScheduleWeek"
        start_date = str(input_start_date)
        dict_parameter = [{"date": start_date}]
        response = requests.post(self.base_url+methode+self.api_key, json=dict_parameter)
        DF_schedule_week = self.ByteToDataframe(response.content)
        if DF_schedule_week.empty == False:
            DF_schedule_week['start_date'] = DF_schedule_week['start_date'].str[0:10]
            DF_schedule_week['end_date'] = DF_schedule_week['end_date'].str[0:10]
        return DF_schedule_week

    def CreateScheduleWeek(self, input_DF_schedule_week):
        methode = "/api/OnDemand/CreateScheduleWeek"
        DF_schedule_week = input_DF_schedule_week
        DF_schedule_week['start_date'] = DF_schedule_week['start_date'].astype(str)
        DF_schedule_week['end_date'] = DF_schedule_week['end_date'].astype(str)
        dict_parameter = DF_schedule_week.to_dict(orient='records')
        requests.post(self.base_url+methode+self.api_key, json=dict_parameter)

    def GetCluster(self):
        methode = "/api/OnDemand/GetClusterMaster"
        response = requests.get(self.base_url + methode + self.api_key)
        DF_cluster = self.ByteToDataframe(response.content)
        return DF_cluster

    def GetSchedules(self, input_schedule_week_id):
        methode = "/api/OnDemand/GetSchedule"
        dict_patameter = [{"id": int(input_schedule_week_id)}]
        response = requests.post(self.base_url + methode + self.api_key, json=dict_patameter)
        DF_schedules = self.ByteToDataframe(response.content)
        if DF_schedules.empty == False:
            DF_schedules["order_forecast_factor"] = DF_schedules["order_forecast_factor"].str.strip().astype(float)
        return DF_schedules

    def CreateSchedule(self, input_DF_schedule):
        methode = "/api/OnDemand/CreateSchedule"
        DF_schedule = input_DF_schedule
        DF_schedule['date_created'] = DF_schedule['date_created'].astype(str)
        DF_schedule["order_forecast_factor"] = DF_schedule["order_forecast_factor"].astype(str)
        dict_patameter = DF_schedule.to_dict(orient='records')
        requests.post(self.base_url + methode + self.api_key, json=dict_patameter)

    def GetClusterStore(self):
        methode = "/api/OnDemand/GetClusterStoreList"
        response = requests.get(self.base_url + methode + self.api_key)
        DF_cluster_store = self.ByteToDataframe(response.content)
        return DF_cluster_store

    def CreatStoreForecast(self, input_DF_store_forecast):
        methode = "/api/OnDemand/CreateStoreForeCast"
        DF_store_forecast = input_DF_store_forecast
        store_id = int(DF_store_forecast['store_id'][0])
        dict_driver_forecasts = DF_store_forecast.to_dict(orient='records')
        dict_parameter = [{"schedule_id": store_id, "driver_forecasts": dict_driver_forecasts}]
        response = requests.post(self.base_url + methode + self.api_key, json=dict_parameter[0])
        print(response.content)

    def GetStoreForecast(self, input_DF_stores):
        methode = "/api/OnDemand/GetStoreForecast"
        dict_parameter = input_DF_stores[['store_id']].rename(columns={'store_id':'id'}).to_dict(orient='records')
        response = requests.post(self.base_url + methode + self.api_key, json=dict_parameter)
        DF_stores_forecast = self.ByteToDataframe(response.content)
        if DF_stores_forecast.empty == False:
            DF_stores_forecast['hour'] = DF_stores_forecast['hour'].str[-8:]
        return DF_stores_forecast

    def GetDriverList(self, active_driver):
        methode = "/api/OnDemand/GetDriverList"
        dict_parameter = {'active':active_driver}
        response = requests.get(self.base_url + methode + self.api_key, params=dict_parameter)
        DF_driver_list = self.ByteToDataframe(response.content)
        return DF_driver_list

    def GetDriverMaster(self):
        methode = "/api/OnDemand/GetInHanceDriverMaster"
        dict_parameter = {'active':True}
        response = requests.get(self.base_url + methode + self.api_key, params=dict_parameter)
        DF_drivers_master = self.ByteToDataframe(response.content)
        return DF_drivers_master

    def GetDriverCluster(self, input_DF_drivers):
        methode = "/api/OnDemand/GetDriverCluster"
        DF_drivers = input_DF_drivers.rename(columns={'driver_id': 'id'})
        DF_drivers = DF_drivers[['id']]
        DF_drivers['id'] = DF_drivers['id'].apply(int)
        dict_parameter = DF_drivers.to_dict(orient='records')
        dict_parameter = [dict([a, int(x)] for a, x in b.items()) for b in dict_parameter]
        response = requests.post(self.base_url + methode + self.api_key, json=dict_parameter)
        DF_driver_cluster = self.ByteToDataframe(response.content)
        return DF_driver_cluster

    def GetDriverShifts(self, input_start_date, input_end_date):
        methode = "/api/OnDemand/GetDriverShifts"
        dict_parameter = {"shift_date_from": str(input_start_date), "shift_date_to": str(input_end_date)}
        response = requests.get(self.base_url + methode + self.api_key, params=dict_parameter)
        DF_schedules = self.ByteToDataframe(response.content)
        return DF_schedules

    def CreatOrderForecast(self, DF_order_forecast):
        methode = "/api/OnDemand/CreateOrderForeCast"
        schedule_id = int(DF_order_forecast['schedule_id'][0])
        #DF_order_forecast['hour'] = "2019-07-13T" + DF_order_forecast['hour'].astype(str).str[0:8] +".021Z"
        DF_order_forecast['orders_requirement'] = DF_order_forecast['orders_requirement'].apply(np.ceil).apply(int)
        dict_order_forecasts = DF_order_forecast.to_dict(orient='records')
        dict_parameter = [{"schedule_id": schedule_id, "order_forecasts": dict_order_forecasts}]
        response = requests.post(self.base_url + methode + self.api_key, json=dict_parameter[0])
        print(response.content)
        time.sleep(2)

    def CreatDriverForecast(self, DF_driver_forecast):
        methode = "/api/OnDemand/CreateDriverForeCast"
        schedule_id = int(DF_driver_forecast['schedule_id'][0])
        DF_driver_forecast['workforce_requirement'] = DF_driver_forecast['workforce_requirement'].apply(np.ceil).apply(int)
        dict_driver_forecasts = DF_driver_forecast.to_dict(orient='records')
        dict_parameter = [{"schedule_id": schedule_id, "driver_forecasts": dict_driver_forecasts}]
        response = requests.post(self.base_url + methode + self.api_key, json=dict_parameter[0])
        print(response.content)
        time.sleep(2)

    def CreatDriverShift(self, DF_driver_shifts):
        methode = "/api/OnDemand/CreateDriverShiftStaging"
        schedule_id = int(DF_driver_shifts['schedule_id'][0])
        dict_driver_shifts = DF_driver_shifts.to_dict(orient='records')
        dict_parameter = [{"schedule_id": schedule_id, "driver_shifts": dict_driver_shifts}]
        response = requests.post(self.base_url + methode + self.api_key, json=dict_parameter[0])
        print(response.content)
        time.sleep(2)

    def GetHistoricalOrders(self, input_start_date, input_end_date):
        methode = "/api/OnDemand/GetHistoricalOrders"
        dict_parameter = {"start": str(input_start_date), "end": str(input_end_date)}
        response = requests.get(self.base_url + methode + self.api_key, params=dict_parameter)
        DF_orders = self.ByteToDataframe(response.content)
        return DF_orders

    def GetStoreParameter(self):
        methode = "/api/OnDemand/GetStoreParameter"
        response = requests.post(self.base_url + methode + self.api_key)
        DF_store_parameter = self.ByteToDataframe(response.content)
        return DF_store_parameter

    def GetClusterParameter(self):
        methode = "/api/OnDemand/GetClusterParameter"
        response = requests.post(self.base_url + methode + self.api_key)
        DF_cluster_parameter = self.ByteToDataframe(response.content)
        return DF_cluster_parameter


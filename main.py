from Schedule.ScheduleManager import *
from Solver.OrderForecast import *
from Solver.DriverForecast import *
from Solver.DriverShiftSolver import *
from Connection.AppProgramingInterfaceRTT import *
from Local.ContentManger import ContentManger
import uuid
import pickle
import traceback


class main_solver():
    def __init__(self, start_date, cluster_id):
        self.user_selected_date = start_date.replace("'", "")
        self.selected_cluster_id = cluster_id
        self.permanent_only = False
        self.DF_schedules = pd.DataFrame()
        self.DF_schedule = pd.DataFrame()
        self.DF_schedule_week = pd.DataFrame()
        self.DF_order_forecast = pd.DataFrame()
        self.DF_driver_forecast = pd.DataFrame()
        self.DF_cluster_forecast = pd.DataFrame()
        self.DF_driver_schedule = pd.DataFrame()
        self.APIRTT = AppProgramingInterfaceRTT()

    def EditForecast(self):
        ContentManger().EditStoreForecast()

    def EditParameters(self):
        ContentManger().EditLocalStorClusterParameters()

    def EnterDate(self):
        ContentManger().CreateSchedulesMenue(input_DF_clusters=self.APIRTT.GetCluster())
        self.CreateSchedule()

    def CreateSchedule(self):
        Schedules = ScheduleManager(start_date = self.user_selected_date)
        self.DF_schedule_week, self.DF_schedules = Schedules.CreateSchedules()

    def CreateOrderForecast(self):
        OrderForecaster = OrderForecast(input_DF_schedule_week=self.DF_schedule_week, input_DF_schedule=self.DF_schedule)
        self.DF_store_forecast, self.DF_order_forecast = OrderForecaster.CreateOrderForcast()

    def CreateClusterForecast(self):
        DriverForecaster = DriverForecast(DF_schedule_input=self.DF_schedule, DF_store_forecast_input=self.DF_order_forecast)
        self.DF_driver_forecast, self.DF_cluster_forecast = DriverForecaster.CreateClusterForecast()

    def CreateDriverShift(self):
        DriverShiftSolver = DriverShiftSolve(DF_schedule_input=self.DF_schedule, DF_schedule_week_input=self.DF_schedule_week, DF_driver_forecast_input=self.DF_cluster_forecast, permanent_only=self.permanent_only)
        self.DF_shift_result, self.DF_driver_schedule = DriverShiftSolver.CreateSchedule()

    def CreateResultReport(self, i):
        ContentManger().CreateExcelReport(input_cluster_id = self.selected_cluster_id,
                                          input_DF_schedule = self.DF_schedule,
                                          input_DF_store_forecaste = self.DF_store_forecast,
                                          input_DF_order_forecaste = self.DF_order_forecast,
                                          input_DF_driver_forecast = self.DF_driver_forecast,
                                          input_DF_shifts = self.DF_shift_result,
                                          input_DF_driver_shifts = self.DF_driver_schedule,
                                          count=i)

    def SolveCluster(self, i):
        self.CreateSchedule()
        self.selected_cluster_id = int(self.selected_cluster_id)
        self.DF_schedule = self.DF_schedules[self.DF_schedules['cluster_id']==self.selected_cluster_id].reset_index(drop=True)
        self.CreateOrderForecast()
        self.CreateClusterForecast()
        self.CreateDriverShift()
        self.CreateResultReport(i)

    def PrepareResult(self):
        Schedules = ScheduleManager(start_date = self.user_selected_date)
        DF_Schedule = Schedules.GetSchedules()
        schedule_id = DF_Schedule[DF_Schedule['cluster_id']==self.selected_cluster_id].reset_index(drop=True)['schedule_id'][0].astype(int)

        self.DF_order_forecast['schedule_id'] = schedule_id
        self.DF_driver_forecast['schedule_id'] = schedule_id
        self.DF_driver_schedule['schedule_id'] = schedule_id
        self.DF_driver_schedule["driver_id"] = self.DF_driver_schedule["driver_id"].apply(int)

        return self.DF_order_forecast, self.DF_driver_forecast, self.DF_driver_schedule


class solve_session():
    def __init__(self, start_date, ls_cluster_id):
        self.start_date = start_date
        self.ls_cluster_id = ls_cluster_id
        self.solve_token_location = os.path.dirname(os.path.realpath(__file__)).replace("\\","/")+"/Local/Files/"
        self.solve_token = self._create_solve_token()
        self.APIRTT = AppProgramingInterfaceRTT()

    def _create_solve_token(self):
        solve_token = str(uuid.uuid4())
        pkl_file = self.solve_token_location + "solve_session.pkl"

        try:
            file = open(pkl_file, "rb")
            dict_master = pickle.load(file)
            file.close()
        except:
            dict_master = {}

        dict_solve_id = {solve_token:{"shift_start_date":self.start_date, "cluster_id":{i:"solve in progress" for i in self.ls_cluster_id}}}
        dict_master = {**dict_master, **dict_solve_id}

        #file = open(pkl_file, "wb")
        with open(pkl_file, "wb") as f:
            pickle.dump(dict_master, f, protocol=pickle.HIGHEST_PROTOCOL)

        return solve_token

    def _update_solver_status(self, cluster_id, solve_status):
        solve_token = self.solve_token
        pkl_file = self.solve_token_location + "solve_session.pkl"

        file = open(pkl_file, "rb")
        dict_master = pickle.load(file)
        file.close()

        dict_master[solve_token]["cluster_id"][cluster_id] = solve_status

        file = open(pkl_file, "wb")
        pickle.dump(dict_master, file, protocol=pickle.HIGHEST_PROTOCOL)
        file.close()

    def _import_results(self, DF_order_forecast, DF_driver_forecast, DF_driver_schedule):
        self.APIRTT.CreatOrderForecast(DF_order_forecast=DF_order_forecast)
        self.APIRTT.CreatDriverForecast(DF_driver_forecast=DF_driver_forecast)
        self.APIRTT.CreatDriverShift(DF_driver_shifts=DF_driver_schedule)

    def run_solver(self):
        DF_order_forecast = pd.DataFrame(columns=['schedule_id', 'store_id', 'week_day', 'hour', 'orders_requirement',
                                                  'cluster_id'])
        DF_driver_forecast = pd.DataFrame(columns=['schedule_id', 'store_id', 'week_day', 'hour', 'orders_requirement',
                                                   'order_forecast_factor', 'out_door_time', 'driver_time_customer',
                                                   'customer_time', 'reallocation_time', 'percent_single_volume',
                                                   'percent_double_volume', 'percent_triple_volume',
                                                   'driver_time_single_order', 'driver_time_double_order',
                                                   'driver_time_triple_order', 'avg_driver_time_single_order',
                                                   'avg_driver_time_double_order', 'avg_driver_time_triple_order',
                                                   'avg_driver_time_order', 'avg_order_driver_time', 'avg_orders_trip',
                                                   'workforce_requirement', 'cluster_id'])
        DF_driver_schedule = pd.DataFrame(columns=['schedule_id', 'date', 'shift_number', 'shift_start_time',
                                                   'shift_end_time', 'shift_type_id', 'store_id',
                                                   'missing_solver_driver_id', 'driver_id', 'cluster_id'])

        # Create results
        for i, ci in enumerate(self.ls_cluster_id):
            try:
                print(f'running cluster {ci}')
                solve_session = main_solver(self.start_date, ci)
                solve_session.CreateSchedule()
                solve_session.SolveCluster(i)
                DF_of, DF_df, DF_ds = solve_session.PrepareResult()
                DF_order_forecast = DF_order_forecast.append(DF_of).reset_index(drop=True)
                DF_driver_forecast = DF_driver_forecast.append(DF_df).reset_index(drop=True)
                DF_driver_schedule = DF_driver_schedule.append(DF_ds).reset_index(drop=True)
                solve_status = "solve complete"
            except Exception as e:
                print(traceback.format_exc())
                print("cluster "+str(ci)+" failed")
                solve_status = "solve failed"

            print(str(ci) + " " + solve_status)
            print(ci)
            print(solve_status)
            self._update_solver_status(cluster_id=ci, solve_status=solve_status)

        # Import Results
        DF_driver_schedule['shift_end_time'] = DF_driver_schedule['shift_end_time'].apply(str).str[11:]
        self._import_results(DF_order_forecast, DF_driver_forecast, DF_driver_schedule)

#
ls_test = [1,2,3,4,5,6,8,9,10,11,13,15,16,17,18,23,29,30,31,32,36,37,38,41,43,44,45,46,47,48,49,50,51,52,54,55,56,57,58,
           59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,87,88,89,90,91,92,93,94,95,96,97,
           98,99,100,101,102,103,104,105,106,107,108,109,110,112,141,142,143,144,145,146,147,148,149,150,151,152,153,
           154,155,156,157,158,159,160,161,162,163,164,165,170,171,172,173,174,175,176,177,178,179,180,181,182,183,184,
           185,187,188,189,190,191,192,193,194,186,195,200,19,196,197,198,26,199,201,202,203]



ls_test = [1,2,3,4,5,6,8,9,10,11,13,14,15,16,17,18,19,23,29,30,31,32,36,37,38,41,43,44,45,46,47,48,49,50,51,54,55,56,
           57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,72,73,74,75,76,77,78,79,80,81,82,83,87,88,89,90,91,92,93,94,95,
           96,97,98,99,100,101,102,103,104,105,106,107,108,109,110,112,141,142,143,144,145,146,147,148,149,150,151,152,
           153,154,155,156,157,158,159,160,161,162,163,164,165,170,171,172,173,174,175,176,177,178,179,180,181,182,183,
           184,185,187,188,189,190,191,192,193,194,195,196,197,198,199,200,201,202,203,206,211,212,213,214,215,216,217,
           218,220,222,223,224,225,227,228,229,230,231,232,233,234]

obj = solve_session(start_date="2022-07-18", ls_cluster_id=ls_test)
obj.run_solver()
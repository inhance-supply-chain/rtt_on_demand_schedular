from Connection.AppProgramingInterfaceRTT import *
from Solver.Models.ShiftsVersion2 import *
from Solver.Models.ResourceVersion2 import *
from Local.ContentManger import *

class DriverShiftSolve():
    def __init__(self, DF_schedule_input, DF_schedule_week_input, DF_driver_forecast_input, permanent_only, cluster_store):
        self.DF_schedule = DF_schedule_input
        self.DF_schedule_week = DF_schedule_week_input
        self.DF_driver_forecast = DF_driver_forecast_input
        self.LocalCon = ContentManger()
        self.APIConRTT = AppProgramingInterfaceRTT()
        self.permanent_only = permanent_only
        self.cluster_store_id = cluster_store
        lpk_loc = os.path.dirname(os.path.realpath(__file__)).replace("\\","/").replace("Solver", "windows_glpk") + "/"
        self.glpk_loc = lpk_loc + "glpk-4.65/w64/glpsol.exe"
        coin_or_loc = os.path.dirname(os.path.realpath(__file__)).replace("\\", "/").replace("Solver", "windows_cbc") + "/"
        self.coin_or_loc = coin_or_loc + "64/cbc.exe"

    def CreateSchedule(self):
        #......(1) Input Data
        schedule_id = self.DF_schedule['schedule_id'][0]
        cluster_id = int(self.DF_schedule['cluster_id'][0])
        #DF_cluster_parameters = self.LocalCon.GetLocalClusterParameters()
        DF_cluster_parameters = self.APIConRTT.GetClusterParameter()
        DF_cluster_parameters["cluster_id"] = DF_cluster_parameters["cluster_id"].apply(int)

        DF_cluster_parameters = DF_cluster_parameters[DF_cluster_parameters['cluster_id']==cluster_id].reset_index(drop=True)

        shift_hour_min = DF_cluster_parameters['shift_hour_min'][0]
        shift_hour_max = DF_cluster_parameters['shift_hour_max'][0]
        week_hour_min = DF_cluster_parameters['driver_hour_min'][0]
        week_hour_max = DF_cluster_parameters['driver_hour_max'][0]

        amount_week_days = 7 - (self.DF_driver_forecast['week_day'].min() - 1)
        DF_demand = self.DF_driver_forecast

        #.......(2) Create Shifts and generate input data to resource allocation model
        Shifts = ShiftSolver(input_DF_driver_forecast=DF_demand,
                             input_hour_intervals=24,
                             input_min_hour_shift_interval=int(shift_hour_min),
                             input_max_hour_shift_interval=int(shift_hour_max), path_gnu_lpk=self.glpk_loc,
                             path_coin_or=self.coin_or_loc)

        DF_shift_result = Shifts.ExecuteModel()

        DF_store = self.APIConRTT.GetClusterStore()
        # .......(2) Create Shifts and generate input data to resource allocation model


        Resource = ResourceSolver(input_DF_shift_result= DF_shift_result,
                                     input_driver_hour_min=week_hour_min,
                                     input_driver_hour_max=week_hour_max,
                                     input_schedule_start_date=self.DF_schedule_week['start_date'][0],
                                     input_cluster_id=cluster_id,
                                     cluster_store_id=self.cluster_store,
                                     input_DF_store=DF_store, path_gnu_lpk=self.glpk_loc, path_coin_or=self.coin_or_loc)

        DF_schedule = Resource.ExecuteModel()

        DF_schedule = self._AllocateDriversToShifts(cluster_id=cluster_id,input_DF_schedule=DF_schedule)

        return DF_shift_result, DF_schedule


    def _AllocateDriversToShifts(self, cluster_id, input_DF_schedule):

        # (1)...First Get Drivers of cluster being solved for
        DF_drivers = self.APIConRTT.GetDriverMaster()
        if self.permanent_only == True:
            DF_drivers = DF_drivers[DF_drivers['driver_is_permanent']==self.permanent_only].reset_index(drop=True)

        DF_drivers_cluster = self.APIConRTT.GetDriverCluster(input_DF_drivers=DF_drivers)

        DF_drivers = DF_drivers_cluster[DF_drivers_cluster['cluster_id']==cluster_id][['driver_id']].reset_index(drop=True)

        # (2)...Order drivers based on least amount of work for past two weeks
        ##
        #
        start_date = datetime.datetime.strptime(input_DF_schedule['date'].min(), "%Y-%m-%d") - datetime.timedelta(days=1)
        end_date = start_date - datetime.timedelta(days=15)

        start_date = datetime.datetime.strftime(start_date, "%Y-%m-%d")
        end_date = datetime.datetime.strftime(end_date, "%Y-%m-%d")
        DF_schedule_history = self.APIConRTT.GetDriverShifts(input_start_date=end_date, input_end_date=start_date)

        if DF_schedule_history.empty == False:
            DF_schedule_history = DF_schedule_history[DF_schedule_history['driver_id'].isin(DF_drivers['driver_id'])].reset_index(drop=True)

            # input_DF_schedule.to_csv('schedule.csv')
            DF_schedule_history['shift_start_time'] = pd.to_timedelta(DF_schedule_history['shift_start_time'].apply(str))
            DF_schedule_history['shift_end_time'] = pd.to_timedelta(DF_schedule_history['shift_end_time'].apply(str))

            DF_schedule_history['shift_hour'] = (DF_schedule_history['shift_end_time'] - DF_schedule_history['shift_start_time']) / np.timedelta64(1, 'h')

            DF_schedule_history = DF_schedule_history.groupby(['driver_id'], as_index=False)['shift_hour'].sum()
            DF_driver_h_rank = DF_schedule_history.sort_values(by=['shift_hour']).reset_index(drop=True)

            if (DF_drivers[~DF_drivers['driver_id'].isin(DF_driver_h_rank['driver_id'])].empty) == False:
                DF_drivers['shift_hour'] = 0
                DF_driver_h_rank = DF_drivers[~DF_drivers['driver_id'].isin(DF_driver_h_rank['driver_id'])].append(DF_driver_h_rank).reset_index(drop=True)

        else:
            DF_driver_h_rank = DF_drivers[['driver_id']]
            DF_driver_h_rank['shift_hour'] = 0


        # (3)...rank shift with most hours
        ##
        DF_sch_driver_h = input_DF_schedule.copy()
        DF_sch_driver_h['shift_start_time'] = pd.to_timedelta(DF_sch_driver_h['shift_start_time'].apply(str))

        DF_sch_driver_h.to_csv('schedule2.csv')
        DF_sch_driver_h['shift_end_time'] = pd.to_timedelta(DF_sch_driver_h['shift_end_time'].apply(str).str[11:])
        DF_sch_driver_h['shift_hour'] = (DF_sch_driver_h['shift_end_time'] - DF_sch_driver_h['shift_start_time'])/np.timedelta64(1, 'h')

        DF_sch_driver_h = DF_sch_driver_h.groupby(['solver_driver_id'], as_index=False)['shift_hour'].sum()
        DF_sch_driver_h  = DF_sch_driver_h.sort_values(by=['shift_hour'], ascending=False).reset_index(drop=True)


        # (4)...Assign Drivers with least working hours to shift of most working hours
        ##
        DF_driver_assign = pd.merge(DF_sch_driver_h[['solver_driver_id']], DF_driver_h_rank[['driver_id']], left_index=True, right_index=True)
        input_DF_schedule["missing_solver_driver_id"] = input_DF_schedule["solver_driver_id"]

        DF_schedule = pd.merge(input_DF_schedule, DF_driver_assign, left_on='solver_driver_id', right_on='solver_driver_id', how='left')
        DF_schedule.loc[DF_schedule["driver_id"].isna(), "driver_id"] = DF_schedule.loc[DF_schedule["driver_id"].isna(), "missing_solver_driver_id"]
        DF_schedule = DF_schedule.drop(columns=['solver_driver_id'])

        return DF_schedule







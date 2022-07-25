from Local.ContentManger import *
from Connection.AppProgramingInterfaceRTT import *
import datetime as dt

class ScheduleManager():
    def __init__(self, start_date):
        self.start_date = dt.datetime.strptime(start_date,'%Y-%m-%d').date()
        self.APIConRTT = AppProgramingInterfaceRTT()
        self.LocalFiles = ContentManger()
        self.DF_schedule_week = self.GetScheduleWeek()

    def _CreateScheduleWeek(self):
        date = self.start_date
        week_number = date.isocalendar()[1]                       # week number in year 1 -> 336
        week_start_day = date.weekday()+1                           # day number in week the schedule will start from 1 -> 7
        days_add = 7 #- (date.weekday()+1) + 1                     # plus 1 because our days are from Tu to Mo
        schedule_start_date = date                                # end date of schedule
        schedule_end_date = date + dt.timedelta(days = days_add)  # end date of schedule

        DF_schedule_week = pd.DataFrame(columns=['week_number', 'week_start_day', 'start_date', 'end_date'],
                                        data=[[week_number, week_start_day, schedule_start_date, schedule_end_date]])

        return DF_schedule_week


    def GetScheduleWeek(self):
        date = self.start_date
        DF_schedule_week = self.APIConRTT.GetScheduleWeek(input_start_date=date)
        if (DF_schedule_week.empty):
            DF_schedule_week = self._CreateScheduleWeek()
            self.APIConRTT.CreateScheduleWeek(input_DF_schedule_week=DF_schedule_week)

        DF_schedule_week = self.APIConRTT.GetScheduleWeek(input_start_date=date)

        return DF_schedule_week.reset_index(drop=True)

    def CreateSchedules(self):
        schedule_week_id = self.DF_schedule_week['schedule_week_id'][0]
        DF_cluster = self.APIConRTT.GetCluster()

        # create Schedule
        DF_schedules = DF_cluster[DF_cluster['cluster_isactive']==True][['cluster_id']]

        DF_schedules['schedule_week_id'] = schedule_week_id
        DF_schedules['schedule_id'] = 1
        DF_schedules = DF_schedules[['schedule_id', 'schedule_week_id', 'cluster_id']].reset_index(drop=True)

        DF_schedules = DF_schedules.assign(**{'date_created': dt.datetime.now(),
                                            'order_forecast_complete': 1,
                                            'driver_forecast_complete': 1,
                                            'driver_shifts_complete': 1,
                                            'schedule_commenced': 1})

        return self.DF_schedule_week, DF_schedules.reset_index(drop=True)


    def GetSchedules(self):
        schedule_week_id = self.DF_schedule_week['schedule_week_id'][0]

        DF_schedules_DB = self.APIConRTT.GetSchedules(input_schedule_week_id=schedule_week_id)
        if (DF_schedules_DB.empty)==True:
            DF_schedule_week, DF_schedules = self.CreateSchedules()
            DF_schedules.insert(3, "order_forecast_factor", 1)
            DF_schedules.insert(4, "odt_time", 1)
            DF_schedules.insert(5, "driver_time_customer", 1)
            DF_schedules.insert(6, "customer_time", 1)
            DF_schedules.insert(7, "reallocation_time", 1)
            DF_schedules.insert(8, "percent_single_volume", 1)
            DF_schedules.insert(9, "percent_double_volume", 1)
            DF_schedules.insert(10, "shift_hour_min", 1)
            DF_schedules.insert(11, "shift_hour_max", 1)
            DF_schedules.insert(12, "driver_hour_min", 1)
            DF_schedules.insert(13, "driver_hour_max", 1)
            self.APIConRTT.CreateSchedule(input_DF_schedule=DF_schedules)
        else:
            DF_schedule_week, DF_schedules = self.CreateSchedules()
            DF_schedules = DF_schedules[~DF_schedules['cluster_id'].isin(DF_schedules_DB['cluster_id'])]
            if (DF_schedules.empty)==False:
                DF_schedules.insert(3, "order_forecast_factor", 1)
                DF_schedules.insert(4, "odt_time", 1)
                DF_schedules.insert(5, "driver_time_customer", 1)
                DF_schedules.insert(6, "customer_time", 1)
                DF_schedules.insert(7, "reallocation_time", 1)
                DF_schedules.insert(8, "percent_single_volume", 1)
                DF_schedules.insert(9, "percent_double_volume", 1)
                DF_schedules.insert(10, "shift_hour_min", 1)
                DF_schedules.insert(11, "shift_hour_max", 1)
                DF_schedules.insert(12, "driver_hour_min", 1)
                DF_schedules.insert(13, "driver_hour_max", 1)
                self.APIConRTT.CreateSchedule(input_DF_schedule=DF_schedules)

        DF_schedules = self.APIConRTT.GetSchedules(input_schedule_week_id=schedule_week_id).reset_index(drop=True)

        return DF_schedules





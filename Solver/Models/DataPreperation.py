import pandas as pd
import datetime
import datetime as dt

class SolverDataPreperation(object):

    def InsertData(self,  DF_demand = None, DF_shift_solution = None, DF_resource_solution = None):
        if isinstance(DF_demand, pd.DataFrame):
            self.DF_demand = DF_demand
        if isinstance(DF_shift_solution  , pd.DataFrame):
            self.DF_shift_solution  = DF_shift_solution
        if isinstance(DF_resource_solution, pd.DataFrame):
            self.DF_resource_solution = DF_resource_solution

    def DeriveShiftResult(self):
        DF_demand = self.DF_demand

        DF_solution = self.DF_shift_solution[['week_day','hour','solution']]

        DF_shift_result = pd.merge(DF_demand, DF_solution, how='left', left_on=['week_day', 'hour'],
                             right_on=['week_day','hour'])  # add to dataframe

        DF_shift_result = DF_shift_result.fillna(0)
        DF_shift_result['solution'] = DF_shift_result['solution'].astype(int)
        DF_shift_result = DF_shift_result.rename(columns={'solution':'amount_drivers_starting_at_time_interval'})
        self.DF_shift_result = DF_shift_result.reset_index(drop=True)
        return self.DF_shift_result

    def DeriveModelResourceInput(self):
        self.DeriveShiftResult()
        DF_shift_result = self.DF_shift_result
        DF_resource_input = DF_shift_result[DF_shift_result['amount_drivers_starting_at_time_interval'] != 0].reset_index(drop=True)
        DF_resource_input = DF_resource_input.reset_index().rename(columns={'index':'shift_id'})
        DF_resource_input['shift_id'] = DF_resource_input['shift_id']+1
        DF_resource_input = DF_resource_input[['shift_id',  'week_day', 'hour', 'amount_drivers_starting_at_time_interval']].reset_index(drop=True)
        self.DF_resource_input = DF_resource_input
        driver_count = DF_resource_input.groupby('week_day').sum().max()['amount_drivers_starting_at_time_interval']
        shift_count = DF_resource_input['amount_drivers_starting_at_time_interval'].count()
        return self.DF_resource_input, shift_count, driver_count

    def DeriveShiftGant(self):
        DF_result = self.DF_shift_result
        amount_week_days = self.DF_demand['week_day'].max()

        DF_result_final = pd.DataFrame(columns=['cluster_id', 'week_day', 'hour', 'workforce_requirement',
                                                'amount_drivers_starting_at_time_interval'])

        DF_demand_gant = DF_result[DF_result['amount_drivers_starting_at_time_interval'] != 0].reset_index(drop=True)
        DF_demand_gant['driver'] = ''
        DF_demand_gant['start_time'] =  DF_demand_gant['hour']
        DF_demand_gant['end_time'] = ( DF_demand_gant['hour'] + pd.Timedelta(hours=self.h))
        DF_demand_gant =  DF_demand_gant.drop(columns=['hour'])
        DF_demand_gant_total_loop = pd.DataFrame(columns= DF_demand_gant.columns)

        for z in range(len(DF_demand_gant)):
            for q in range(int(DF_demand_gant['amount_drivers_starting_at_time_interval'][z])):
                DF_demand_gant_griver_i = DF_demand_gant.loc[[z], DF_demand_gant.columns]
                DF_demand_gant_total_loop = DF_demand_gant_total_loop.append(DF_demand_gant_griver_i)

        DF_demand_gant_total_loop = DF_demand_gant_total_loop.reset_index(drop=True)
        DF_demand_gant_total_loop['driver'] = DF_demand_gant_total_loop.index + 1


    def DeriveResourceResult(self):
        DF_resource_solution = self.DF_resource_solution

        DF_resource_result = DF_resource_solution[DF_resource_solution['solution']!=0].drop(columns=['solution'])
        self.DF_resource_result = DF_resource_result.sort_values(by=['shift_id', 'driver_id'])[['shift_id','driver_id']].reset_index(drop=True)

        return self.DF_resource_result

    def CreateSchedule(self, DF_schedule_week, cluster_id, schedule_id, shift_hour_min):

        DF_shift = self.DF_resource_input

        DF_resource =  self.DeriveResourceResult()
        schedule_start_date = DF_schedule_week['start_date'][0]

        DF_shift = DF_shift.drop(columns=['shift_number_start', 'shift_number_end'])


        DF_schedule = pd.merge(DF_shift,DF_resource,left_on='shift_id',right_on='shift_id',how='left')
        DF_schedule = DF_schedule.rename(columns={'shift_id':'shift_number'})

        DF_schedule['schedule_id'] = schedule_id

        DF_schedule['date'] = schedule_start_date
        for i in range(len(DF_schedule['date'])):
            DF_schedule.loc[i,'date'] = datetime.datetime.strptime(DF_schedule.loc[i,'date'], "%Y-%m-%d") +datetime.timedelta(days=int(DF_schedule['week_day'][i])-1)

        DF_schedule['cluster_id'] = cluster_id
        DF_schedule['date'] = pd.to_datetime(DF_schedule['date']).dt.strftime('%Y-%m-%d')
        DF_schedule['hour'] = DF_schedule['hour'].str[0:8]
        DF_schedule['hour'] = pd.to_datetime(DF_schedule['hour'], format='%H:%M:%S')
        DF_schedule['shift_start_time'] = DF_schedule['hour'].dt.strftime('%H:%M:%S')
        DF_schedule['shift_end_time'] = (DF_schedule['hour'] + pd.Timedelta(hours=int(shift_hour_min))).dt.strftime('%H:%M:%S')
        DF_schedule['shift_type_id'] = 141
        DF_schedule['store_id'] = 1

        DF_schedule = DF_schedule.reset_index().rename(columns={'index':'shift_id'})
        DF_schedule['shift_id'] =  DF_schedule['shift_id']+1
        self.DF_schedule =  DF_schedule[['schedule_id', 'date', 'shift_number', 'shift_start_time', 'shift_end_time',
                             'driver_id', 'shift_type_id', 'store_id']]

        return self.DF_schedule














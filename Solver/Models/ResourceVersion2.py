from Solver.Resources.Libraries import *
import datetime
import time

class ResourceSolver():

    def __init__(self, input_DF_shift_result, input_driver_hour_min, input_driver_hour_max, input_schedule_start_date, input_cluster_id, input_DF_store, path_gnu_lpk, path_coin_or, cluster_store_id):
        self.DF_shift_result = input_DF_shift_result[input_DF_shift_result['amount_drivers_starting_at_time_interval']!=0].reset_index(drop=True)
        self.driver_hour_min = input_driver_hour_min
        self.driver_hour_max = input_driver_hour_max
        self.schedule_start_date = input_schedule_start_date
        self.cluster_id = input_cluster_id
        self.DF_store = input_DF_store
        self.path_gnu_lpk = path_gnu_lpk
        self.path_coin_or = path_coin_or
        self.cluster_store_id = cluster_store_id

        amount_drivers_shift_const = input_DF_shift_result.groupby('week_day').sum().max()[ 'amount_drivers_starting_at_time_interval']
        total_driver_time_req = (input_DF_shift_result['shift_len']*input_DF_shift_result['amount_drivers_starting_at_time_interval']).sum()
        amount_drivers_time_const = math.ceil(total_driver_time_req/input_driver_hour_max)

        print('DRIVERS REQUIRED AS SHIFT CONSTRAINT: ' + str(amount_drivers_shift_const))
        print('DRIVERS REQUIRED AS TIME CONSTRAINT: ' + str(amount_drivers_time_const))


        self.amount_drivers = int(max(amount_drivers_shift_const, amount_drivers_time_const))
        self.amount_shifts = self.DF_shift_result['amount_drivers_starting_at_time_interval'].count()

    def _ProblemDefinition(self):
        model_dev =  plp.LpProblem(name="MIP_SS_Model", sense=plp.LpMinimize)
        return model_dev

    def _DecisionVariables(self, amount_drivers, amount_shifts):
        set_I = range(1, amount_drivers + 1)
        set_J = range(1, amount_shifts + 1)
        x_vars = {(i, j): plp.LpVariable(cat=plp.LpInteger, name="x_{0}_{1}".format(i, j), lowBound=0, upBound=1) for i in set_I for j in set_J}
        return x_vars

    def _ObjectiveFunction(self, amount_drivers, amount_shifts, x_vars):
        set_I = range(1, amount_drivers + 1)
        set_J = range(1, amount_shifts + 1)
        object_func = plp.lpSum(x_vars[i, j] for i in set_I for j in set_J)

        return object_func

    def _Constraint(self, amount_drivers, amount_shifts, x_vars, DF_shift_result, driver_hour_min, driver_hour_max):
        # data preparation
        set_I = range(1, amount_drivers + 1)
        set_J = range(1, amount_shifts + 1)

        demand = {j: DF_shift_result['amount_drivers_starting_at_time_interval'][j - 1] for j in set_J}
        shift_len = {j: DF_shift_result['shift_len'][j - 1] for j in set_J}

        DF_demand = DF_shift_result.copy()
        DF_demand['shift_number_start'] = DF_demand.index + 1
        DF_demand['shift_number_end'] = DF_demand.index + 1
        shift_group = DF_demand.groupby(['week_day']).agg(
            {'week_day': 'first', 'shift_number_start': 'min', 'shift_number_end': 'max'}).reset_index(drop=True)

        #
        # Constraints
        #
        # one shift a day constraint
        constraints_one_shift_day = {}
        for m in range(len(shift_group.index)):
            constraints_ver = {str("CON_1SD_" +str(i+m*amount_drivers)): plp.LpConstraint(
                e=plp.lpSum(x_vars[i, j] for j in
                            range(shift_group['shift_number_start'][m], shift_group['shift_number_end'][m] + 1)),
                sense=plp.LpConstraintLE,
                rhs=1,
                name="constraint_{0}".format(i)) for i in set_I}
            constraints_one_shift_day = {**constraints_one_shift_day, **constraints_ver}

        # constraint shift demand Bj must be met
        constraints_demand = {str("CON_D_"+ str(j)): plp.LpConstraint(
            e=plp.lpSum(x_vars[i, j] for i in set_I),
            sense=plp.LpConstraintGE,
            rhs= demand[j],
            name="constraint_{0}".format(j)) for j in set_J}
        #print(constraints_demand)

        # constraint workforce conly work 6 days a week
        constraints_working_days = {str("CON_WorkDays"+str(i)): plp.LpConstraint(
            e=plp.lpSum(x_vars[i, j] for j in set_J),
            sense=plp.LpConstraintLE,
            rhs=6,
            name="constraint_{0}".format(i)) for i in set_I}
        #print(constraints_working_days)

        # constraint workforce must work more than 40 hours
        constraints_max_hour = {str("CON_MaH"+str(i)): plp.LpConstraint(
            e=plp.lpSum((shift_len[j]) * x_vars[i, j] for j in set_J),
            sense=plp.LpConstraintGE,
            rhs=driver_hour_min,
            name="constraint_{0}".format(i)) for i in set_I}
        #print(constraints_max_hour)

        # constraint workforce must work less than 60 hours
        constraints_min_hour = {str("CON_MiH" + str(i)): plp.LpConstraint(
            e=plp.lpSum((shift_len[j]) * x_vars[i, j] for j in set_J),
            sense=plp.LpConstraintLE,
            rhs=driver_hour_max,
            name="constraint_{0}".format(i)) for i in set_I}
        #print(constraints_min_hour)

        constraints ={**constraints_one_shift_day, **constraints_demand,
                      **constraints_max_hour, **constraints_min_hour,
                      **constraints_working_days}
        return constraints

    def _SolveResourceModel(self):
        amount_drivers = self.amount_drivers
        amount_shifts = self.amount_shifts
        DF_shift_result = self.DF_shift_result
        driver_hour_min = self.driver_hour_min
        driver_hour_max = self.driver_hour_max
        run_model = True

        while run_model:
            model_dev = self._ProblemDefinition()
            x_vars = self._DecisionVariables(amount_drivers, amount_shifts)
            object_func = self._ObjectiveFunction(amount_drivers, amount_shifts, x_vars)
            constraints = self._Constraint(amount_drivers, amount_shifts, x_vars, DF_shift_result, driver_hour_min, driver_hour_max)

            opt_model = model_dev
            opt_model.objective = object_func
            opt_model.constraints = constraints
            # opt_model.solve(plp.GLPK(path=self.path_gnu_lpk, msg = 0, options=["--tmlim", "120"]))    #  plp.GLPK(msg = 1, options=["--tmlim", "120"])
            opt_model.solve(plp.COIN_CMD(path=self.path_coin_or, msg=False, timeLimit=120))

            if opt_model.status == 1:
                run_model = False
            else:
                run_model = True
                print("RUN 1")
                amount_drivers = amount_drivers+1

        print("Requires :"+str(amount_drivers)+" drivers")
        return opt_model, x_vars

    def _GenerateResourceModelResult(self, x_vars):
        DF_solver_output = pd.DataFrame.from_dict(x_vars, orient="index", columns=["variable"])
        DF_solver_output.index = pd.MultiIndex.from_tuples(DF_solver_output.index, names=["column_i", "column_j", ])
        DF_solver_output.reset_index(inplace=True)
        DF_solver_output["solution"] = DF_solver_output["variable"].apply(lambda item: item.varValue)

        DF_resource_result = DF_solver_output[DF_solver_output['solution']!=0].drop(columns=['solution', 'variable'])
        DF_resource_result = DF_resource_result.rename(columns={'column_j':'shift_id', 'column_i':'solver_driver_id'})
        DF_resource_result = DF_resource_result[['shift_id', 'solver_driver_id']].sort_values(by=['shift_id', 'solver_driver_id']).reset_index(drop=True)

        DF_shift = self.DF_shift_result[['schedule_id','week_day', 'hour', 'amount_drivers_starting_at_time_interval', 'shift_len']]
        DF_shift = DF_shift.reset_index().rename(columns={'index':'shift_id'})
        DF_shift['shift_id'] = DF_shift['shift_id']+1

        DF_schedule = pd.merge(DF_shift,DF_resource_result,left_on='shift_id',right_on='shift_id',how='left')
        DF_schedule = DF_schedule.rename(columns={'shift_id':'shift_number'})

        DF_schedule['date'] = self.schedule_start_date
        for i in range(len(DF_schedule['date'])):
            DF_schedule.loc[i, 'date'] = datetime.datetime.strptime(DF_schedule.loc[i, 'date'], "%Y-%m-%d") + datetime.timedelta(days=int(DF_schedule['week_day'][i]) - 1)

        DF_schedule['cluster_id'] = self.cluster_id
        DF_schedule['date'] = pd.to_datetime(DF_schedule['date']).dt.strftime('%Y-%m-%d')
        DF_schedule['hour'] = DF_schedule['hour'].str[-8:]
        DF_schedule['hour'] = pd.to_datetime(DF_schedule['hour'], format='%H:%M:%S')
        DF_schedule['shift_start_time'] = DF_schedule['hour'].dt.strftime('%H:%M:%S')
        DF_schedule['shift_end_time'] = DF_schedule['hour'] #+ pd.Timedelta(hours=int(8))).dt.strftime('%H:%M:%S')
        for i in range(len(DF_schedule['date'])):
            DF_schedule.loc[i,'shift_end_time'] = str(DF_schedule['hour'][i] + pd.Timedelta(hours=int(DF_schedule['shift_len'][i])))[-8:]   #.dt.strftime('%H:%M:%S')

        DF_schedule = DF_schedule.drop(columns=['shift_len'])
        DF_schedule['shift_type_id'] = 141

        # Get store and select Checkers first if exists
        DF_store_id = self.DF_store[self.DF_store['cluster_id'] == int(self.cluster_id)][['store_id', 'store_description']]
        DF_store_id["contains_checkers"] = DF_store_id["store_description"].apply(lambda x: 1 if x.lower().find("checkers") >= 0 else 0)

        if sum(DF_store_id["contains_checkers"]) >= 1:
            DF_store_id = DF_store_id[DF_store_id["contains_checkers"] == 1].reset_index(drop=True)
            DF_store_id = DF_store_id[~DF_store_id["store_description"].str.lower().str.contains('liquorshop')].reset_index(drop=True)
            if len(DF_store_id) > 1:
                store_id = self.cluster_store_id
            else:
                store_id = DF_store_id['store_id'].iloc[0]
        else:
            DF_store_id = DF_store_id.reset_index(drop=True)
            store_id = DF_store_id['store_id'].iloc[0]

        DF_schedule['store_id'] = store_id

        DF_schedule = DF_schedule.reset_index().rename(columns={'index':'shift_id'})
        DF_schedule['shift_id'] =  DF_schedule['shift_id']+1
        DF_schedule =  DF_schedule[['schedule_id', 'date', 'shift_number', 'shift_start_time', 'shift_end_time',
                             'solver_driver_id', 'shift_type_id', 'store_id']]

        return DF_schedule

    def ExecuteModel(self):
        opt_model, x_vars = self._SolveResourceModel()
        DF_schedule = self._GenerateResourceModelResult(x_vars)

        return DF_schedule

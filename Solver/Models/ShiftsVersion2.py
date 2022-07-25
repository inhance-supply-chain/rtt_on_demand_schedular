from Solver.Resources.Libraries import *

class ShiftSolver():

    def __init__(self, input_DF_driver_forecast, input_hour_intervals, input_min_hour_shift_interval, input_max_hour_shift_interval, path_gnu_lpk, path_coin_or):
        self.day_inter = input_hour_intervals  # amount of hourly intervals  (current 12)
        self.min_h_s = input_min_hour_shift_interval
        self.max_h_s = input_max_hour_shift_interval
        self.DF_driver_forecast = input_DF_driver_forecast
        self.path_gnu_lpk = path_gnu_lpk
        self.path_coin_or = path_coin_or

    def _ProblemDefinition(self):
        model_dev = plp.LpProblem(name="LP_Multiperiod_Work_Scheduling_Problem", sense=plp.LpMinimize)
        return model_dev

    def _DecisionVariables(self, min_h_s, max_h_s, day_inter):
        variable_collection = {}
        shift_diff = max_h_s - min_h_s
        for j in range(shift_diff + 1):
            h = min_h_s + j
            s = day_inter - h + 1
            set_S = range(1, s + 1)
            x_vars = {(h, s): plp.LpVariable(cat=plp.LpInteger, name="x_{0}_{1}".format(h, s), lowBound=0, upBound=90)
                      for s in set_S}
            variable_collection = {**variable_collection, **x_vars}
        x_vars = variable_collection

        return x_vars

    def _ObjectiveFunction(self, min_h_s, max_h_s, day_inter, x_vars):
        objective_collection = {}
        shift_diff = max_h_s - min_h_s
        for j in range(shift_diff + 1):
            h = min_h_s + j
            s = day_inter - h + 1
            set_S = range(1, s + 1)
            objective_collection[j] = plp.lpSum(h*x_vars[h, s] for s in set_S)
        object_func = objective_collection[0]
        if shift_diff >= 1:
            for n in range(len(objective_collection.keys()) - 1):
                object_func = object_func + objective_collection[n + 1]
        #print(object_func)
        return object_func

    def _Constraint(self, min_h_s, max_h_s, day_inter, x_vars, DF_driver_forecast_i):
        all_constraints_collection = {}
        shift_diff = max_h_s - min_h_s
        for n in range(shift_diff + 1):
            i = day_inter
            h = min_h_s + n
            s = day_inter - h + 1
            if h == min_h_s:
                W = DF_driver_forecast_i['workforce_requirement']
            else:
                DF_driver_forecast_i['workforce_requirement_zero'] = 0
                W = DF_driver_forecast_i['workforce_requirement_zero']
            constraints_collection = {}
            if h > s:
                for k in range(1, i + 1):
                    if (k <= s):
                        constraintsV = {k: plp.LpConstraint(
                            e=plp.lpSum(1 * x_vars[h, j] for j in range(1, k + 1)),
                            sense=plp.LpConstraintGE,
                            rhs=W[k - 1],  # b[j],
                            name="constraint_{0}".format(s))}
                        #print(constraintsV)
                    elif ((k > s) and k <= (i - s)):
                        constraintsV = {k: plp.LpConstraint(
                            e=plp.lpSum(1 * x_vars[h, j] for j in range(1, 1 + s)),
                            sense=plp.LpConstraintGE,
                            rhs=W[k - 1],  # b[j],
                            name="constraint_{0}".format(s))}
                        #print(constraintsV)
                    elif ((k > i - s)):
                        constraintsV = {k: plp.LpConstraint(
                            e=plp.lpSum(1 * x_vars[h, j] for j in range((k - (i - s)), 1 + s)),
                            sense=plp.LpConstraintGE,
                            rhs=W[k - 1],  # b[j],
                            name="constraint_{0}".format(s))}
                        #print(constraintsV)
                    constraints_collection = {**constraints_collection, **constraintsV}
            elif h < s:
                for k in range(1, i + 1):
                    if (k <= h):
                        constraintsV = {k: plp.LpConstraint(
                            e=plp.lpSum(1 * x_vars[h, j] for j in range(1, k + 1)),
                            sense=plp.LpConstraintGE,
                            rhs=W[k - 1],  # b[j],
                            name="constraint_{0}".format(s))}
                        #print(constraintsV)
                    elif ((k > h)):
                        if i - k >= h:
                            max = k + 1
                        else:
                            max = i - h + 2
                        constraintsV = {k: plp.LpConstraint(
                            e=plp.lpSum(1 * x_vars[h, j] for j in range(k - h + 1, max)),
                            sense=plp.LpConstraintGE,
                            rhs=W[k - 1],  # b[j],
                            name="constraint_{0}".format(s))}
                        #print(constraintsV)
                    constraints_collection = {**constraints_collection, **constraintsV}

            all_constraints_collection[n + 1] = constraints_collection

        #print(all_constraints_collection)

        c = 0

        if len(all_constraints_collection[1])==0:
            for mk in range(len(all_constraints_collection.keys())-1):
                all_constraints_collection[mk+1] = all_constraints_collection[mk+2]

        constraints = {}
        if (shift_diff == 0):
            constraints = all_constraints_collection[1]
        else:
            constraints = all_constraints_collection[1]
            for s in range(shift_diff):
                if (not all_constraints_collection[s + 2])==False:
                    for a in range(i):
                        if (constraints[a + 1])==False:
                            constraints[a + 1] = constraints[a + 1] + all_constraints_collection[s + 2][a + 1]
        #print(constraints)
        return constraints

    def _SolveShiftsModel(self, input_DF_driver_forecast_I):
        day_inter = input_DF_driver_forecast_I.index.max()+1
        min_h_s = self.min_h_s
        max_h_s = self.max_h_s
        DF_driver_forecast = input_DF_driver_forecast_I

        model_dev = self._ProblemDefinition()
        x_vars = self._DecisionVariables(min_h_s, max_h_s, day_inter)
        object_func = self._ObjectiveFunction(min_h_s, max_h_s, day_inter, x_vars)
        constraints = self._Constraint(min_h_s, max_h_s, day_inter, x_vars, DF_driver_forecast)

        opt_model = model_dev
        opt_model.objective = object_func
        constraints = {"hour_{}".format(str(i)): constraints[i] for i in list(constraints.keys())}
        opt_model.constraints = constraints
        # opt_model.solve(plp.GLPK(path=self.path_gnu_lpk, msg=0))
        opt_model.solve(plp.COIN_CMD(path=self.path_coin_or, msg=False))

        return opt_model, x_vars

    def _GenerateShiftsModelResult(self, input_DF_driver_forecast_day, x_vars):
        DF_solver_output = pd.DataFrame.from_dict(x_vars, orient="index", columns=["variable"])
        DF_solver_output.index = pd.MultiIndex.from_tuples(DF_solver_output.index, names=["column_i", "column_j", ])

        DF_solver_output.reset_index(inplace=True)
        DF_solver_output["solution"] = DF_solver_output["variable"].apply(lambda item: item.varValue)

        DF_solver_result = DF_solver_output[(DF_solver_output['solution'] > 0)].reset_index(drop=True)
        DF_solver_result = DF_solver_result.rename(columns={'column_i':'shift_len',
                                                            'column_j':'day_interval',
                                                            'solution':'amount_drivers_starting_at_time_interval'})

        DF_input_data = input_DF_driver_forecast_day

        DF_input_data['day_interval'] = DF_input_data.index
        DF_input_data['day_interval'] = DF_input_data['day_interval']+1
        DF_solver_result = pd.merge(DF_input_data[['schedule_id', 'week_day', 'day_interval', 'hour', 'workforce_requirement']],
                                    DF_solver_result[['day_interval', 'amount_drivers_starting_at_time_interval', 'shift_len']],
                                    left_on='day_interval', right_on='day_interval', how='left')


        DF_solver_result['amount_drivers_starting_at_time_interval'] = DF_solver_result['amount_drivers_starting_at_time_interval'].fillna(0).astype(int)
        DF_solver_result['shift_len'] = DF_solver_result['shift_len'].fillna(0).astype(int)

        return DF_solver_result

    def ExecuteModel(self):
        DF_driver_forcast = self.DF_driver_forecast
        week_days = DF_driver_forcast['week_day'].drop_duplicates(keep='first').reset_index(drop=True)
        amount_week_days = len(week_days)

        max_hour_shift = self.max_h_s
        min_hour_shift = self.min_h_s

        shift_result_collections = {}
        for i in range(amount_week_days):
            week_day = week_days[i]
            DF_driver_forecast_I = DF_driver_forcast[DF_driver_forcast['week_day']==week_day].reset_index(drop=True)

            if len(DF_driver_forecast_I['hour']) <max_hour_shift:
                self.max_h_s = len(DF_driver_forecast_I['hour'])
            elif len(DF_driver_forecast_I['hour']) >= max_hour_shift:
                self.max_h_s = max_hour_shift

            if len(DF_driver_forecast_I['hour']) <min_hour_shift:
                self.min_h_s = len(DF_driver_forecast_I['hour'])
            elif len(DF_driver_forecast_I['hour']) >= min_hour_shift:
                self.min_h_s = min_hour_shift


            opt_model, x_vars = self._SolveShiftsModel(DF_driver_forecast_I)
            DF_shift_result_I = self._GenerateShiftsModelResult(DF_driver_forecast_I, x_vars)
            shift_result_collections[i] = DF_shift_result_I

        DF_shift_result = shift_result_collections[0]
        for i in range(1, amount_week_days):
            DF_shift_result = DF_shift_result.append(shift_result_collections[i]).reset_index(drop=True)

        return DF_shift_result




#DF_demand = pd.read_csv("input_DF_demand.csv")
##DF_demand = DF_demand[DF_demand["week_day"]].reset_index(drop=True)

#Shifts = Shifts(input_DF_driver_forecast = DF_demand, input_hour_intervals=12, input_min_hour_shift_interval=6, input_max_hour_shift_interval=9)

#Shifts.ExecuteModel()
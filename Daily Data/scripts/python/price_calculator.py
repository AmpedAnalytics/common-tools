import pandas as pd

class wholesale_electricy_price:

    def __init__(self, data, nem=True, wem=False):
        '''
        INIT - Read in Data
        
        '''
        self.df = pd.read_csv(data)

        timestamp = pd.Timestamp.today()
        self.today = timestamp.strftime("%Y-%m-%d")
        self.yesterday = (timestamp-pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        self.quarter = timestamp.quarter
        self.current_year = timestamp.year

        self.isNem = nem
        self.isWem = wem
        self.isDKIS = (wem==False and nem==False)

        if nem and wem:
            print("WARNING: Price can't be for NEM and WEM. Defaulting to NEM...")
            wem = False
        
        if nem:
            # Prices for each region
            price = self.df.pivot_table(values='Price', index='DateStamp', 
                                columns='Region', aggfunc='first')

            # Demand for each region
            demand = self.df.pivot_table(values='Demand', index='DateStamp',
                                columns='Region', aggfunc='first')
        if not nem:
            
            price = self.df.pivot_table(values='Price', index='DateStamp', 
                                        aggfunc='first')
            price.index = pd.to_datetime(price.index) 

            if wem:
                demand = self.df.pivot_table(values='Demand', index='DateStamp',
                                         aggfunc='first')
                self.peak_demand = demand.query(f"index >= '{self.yesterday} 00:05' \
                                and index <= '{self.today} 00:00'").max() # FILTER DATES
                price.index = pd.to_datetime(price.index) - pd.Timedelta(hours=2)
                demand = pd.DataFrame({"Demand":[1]*len(price)},index=price.index)
            else:
                demand = self.df.pivot_table(values='Demand', index='DateStamp',
                                         aggfunc='first')
                
        # Convert to datetime
        price.index = pd.to_datetime(price.index) 
        demand.index = pd.to_datetime(demand.index) 

        self.price = price
        self.demand = demand
        self.last_dkis_day = price.index[-1].strftime("%Y-%m-%d")
    
    def _lin_average(self, startdate, enddate):
        '''
        Test with demand being a linear average, as MW is 
        the instantaneous value measured at the time interval.

        new xx:xx demand := (xx:xx demand + xx:xx-00:05 demand) / 2
        '''
        demand = self.demand.query(f"index >= '{startdate} 00:00' \
                            and index <= '{enddate} 00:00'") # FILTER DATES
        demand = demand.rolling('10 min').mean()
        demand = demand.query(f"index >= '{startdate} 00:05'")

        return demand.copy()
    
    def _get_offset_date(self,today,year):
        offset = self.current_year - year
        if offset <= 0:
            today = pd.to_datetime(today)
        else:
            today = pd.to_datetime(today) - pd.offsets.DateOffset(years=offset)
        return today

    def _report_date(self,day):
        return (day-pd.Timedelta(days=1)).strftime("%Y-%m-%d")
    
    def _calculate_price(self,start,end,linear_average,peak_demand=False):

        price = self.price.query(f"index >= '{start} 00:05' \
                        and index <= '{end} 00:00'") # FILTER DATES

        if linear_average:
            demand = self._lin_average(start,end)
        else:
            demand = self.demand.query(f"index >= '{start} 00:05' \
                                and index <= '{end} 00:00'").copy() # FILTER DATES

        if self.isNem:
            VWA = demand*price # VOLUME WEIGHTIFY THE REGIONAL PRICES WITH DEMAND (MW)
            
            demand['NEM'] = demand.sum(axis=1) # Total NEM demand
            VWA['NEM'] = VWA.sum(axis=1) # Volume weighted NEM prices
        else:
            VWA = pd.DataFrame({"Other":price.Price*demand.Demand})
            # print(VWA)
            demand.columns = ["Other"]
        
        VWA = VWA.sum() / demand.sum() # Volume weighted average

        peak = demand.max()

        if peak_demand:
            return VWA.copy().round(2), peak
        else:
            return VWA.copy().round(2)
        
    def update_today(self, today):
        self.today = today
        self.yesterday = (pd.to_datetime(today)-pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        self.reporting_date = self.yesterday

    def latest_available(self, today='', linear_average=False, verbose=False):
        if not today:
            today = self.today
        
        yesterday = (pd.to_datetime(today) - pd.Timedelta(days=1)).strftime("%Y-%m-%d")
        self.reporting_date = yesterday

        if self.isWem:
            VWA = self._calculate_price(yesterday,today,linear_average)
        else:
            VWA, self.peak_demand = self._calculate_price(yesterday,today,linear_average,peak_demand=True)

        if verbose:
            print(f'24 hour weighted average Wholesale Electricity Prices for \
                  {pd.to_datetime(self.reporting_date).strftime("%B %d %Y")}')
            print(VWA.round(2))

        return VWA.round(2)
    
    def fixed_rolling_average(self, start, end, days=28, year=2024, linear_average=False, verbose=False):
        start = self._get_offset_date(start,year)
        end = self._get_offset_date(end,year)

        fakestart = pd.to_datetime(start)-pd.Timedelta(days=days+1)

        price = self.price.query(f"index >= '{fakestart} 00:05' \
                        and index <= '{end} 00:00'") # FILTER DATES

        if linear_average:
            demand = self._lin_average(fakestart,end)
        else:
            demand = self.demand.query(f"index >= '{fakestart} 00:05' \
                                and index <= '{end} 00:00'").copy() # FILTER DATES

        if self.isNem:
            VWA = demand*price # VOLUME WEIGHTIFY THE REGIONAL PRICES WITH DEMAND (MW)
            
            demand['NEM'] = demand.sum(axis=1) # Total NEM demand
            VWA['NEM'] = VWA.sum(axis=1) # Volume weighted NEM prices
        else:
            VWA = pd.DataFrame({"Other":price.Price*demand.Demand})
            demand.columns = ["Other"]

        # prepare dfs for resampling
        if self.isNem:
            VWA.index = VWA.index - pd.Timedelta(minutes=5)
            demand.index = demand.index - pd.Timedelta(minutes=5)
        else:
            VWA.index = VWA.index - pd.Timedelta(minutes=30)
            demand.index = demand.index - pd.Timedelta(minutes=30)

        VWA = VWA.resample('1d').sum()
        demand = demand.resample('1d').sum()

        VWA = VWA.rolling('28D').sum()
        demand = demand.rolling('28D').sum()
        
        VWA = VWA / demand # Volume weighted average

        VWA = VWA.query(f"index >= '{start} 00:00'") # FILTER DATES

        VWA =VWA.transpose()
        
        if verbose:
            print(f'28 Day Rolling Weighted Average Wholesale Electricity Prices for \
                  {pd.to_datetime(start).strftime("%B %d")} to {pd.to_datetime(end).strftime("%B %d %Y %H:%M")}')
            print(VWA.round(2))
        
        return VWA.round(2)
    
    def mtd(self, today='', year=2024, linear_average=False, verbose=False):
        if not today:
            today = self._get_offset_date(self.today,year)
        else:
            today = self._get_offset_date(today,year)
        
        self.reporting_date = self._report_date(today)

        current_period = today.to_period("M")
        month_start = current_period.start_time.strftime("%Y-%m-%d")
        today = today.strftime("%Y-%m-%d")
        if today == month_start:
            month_start = (current_period-1).start_time.strftime("%Y-%m-%d")

        VWA = self._calculate_price(month_start,today,linear_average)

        if verbose:
            print(f'MTD weighted average Wholesale Electricity Prices for \
                  {pd.to_datetime(month_start).strftime("%B %d")} to {pd.to_datetime(self.reporting_date).strftime("%B %d %Y")}')
            print(VWA.round(2))

        return VWA.round(2)
    
    def full_month(self, today='', year=2024, linear_average=False, verbose=False):
        if not today:
            today = self._get_offset_date(self.today,year)
        else:
            today = self._get_offset_date(today,year)
        
        self.reporting_date = self._report_date(today)

        current_period = today.to_period("M")
        month_start = current_period.start_time.strftime("%Y-%m-%d")
        month_end = (current_period+1).start_time.strftime("%Y-%m-%d")
        today = today.strftime("%Y-%m-%d")
        if today == month_start:
            month_start = (current_period-1).start_time.strftime("%Y-%m-%d")
            month_end = today

        VWA = self._calculate_price(month_start,month_end,linear_average)

        if verbose:
            print(f'Full Monthly weighted average Wholesale Electricity Prices for \
                  {pd.to_datetime(month_start).strftime("%B %Y")}')
            print(VWA.round(2))

        return VWA.round(2)
    
    def qtd(self, today='', year=2024, linear_average=False, verbose=False):
        if not today:
            today = self._get_offset_date(self.today,year)
        else:
            today = self._get_offset_date(today,year)
        
        self.reporting_date = self._report_date(today)

        current_period = today.to_period("Q")
        quarter_start = current_period.start_time.strftime("%Y-%m-%d")
        today = today.strftime("%Y-%m-%d")
        if today == quarter_start:
            quarter_start = (current_period-1).start_time.strftime("%Y-%m-%d")

        VWA = self._calculate_price(quarter_start,today,linear_average)

        if verbose:
            print(f'QTD weighted average Wholesale Electricity Prices for \
                  {pd.to_datetime(quarter_start).strftime("%B %d")} to {pd.to_datetime(self.reporting_date).strftime("%B %d %Y")}')
            print(VWA.round(2))

        return VWA.round(2)
        
    def full_qt(self, today='', year=2023, linear_average=False, verbose=False):
        if not today:
            today = self._get_offset_date(self.today,year)
        else:
            today = self._get_offset_date(today,year)
        
        current_period = today.to_period("Q")
        quarter_start = current_period.start_time.strftime("%Y-%m-%d")
        quarter_end = (current_period+1).start_time.strftime("%Y-%m-%d")
        today = today.strftime("%Y-%m-%d")
        if today == quarter_start:
            quarter_start = (current_period-1).start_time.strftime("%Y-%m-%d")
            quarter_end = today
        
        VWA = self._calculate_price(quarter_start,quarter_end,linear_average)

        if verbose:
            print(f'''Full QT weighted average Wholesale Electricity Prices for \
                  {pd.to_datetime(quarter_start).strftime("%B %Y")}''')
            print(VWA.round(2))

        return VWA.round(2)
    
    def ytd(self, today='', year_start=2024, linear_average=False, verbose=False):
        if not today:
            today = self._get_offset_date(self.today,year_start)
        else:
            today = self._get_offset_date(today,year_start)

        self.reporting_date = self._report_date(today)

        if pd.to_datetime(today).strftime("%m-%d") == "01-01":
            year_start = pd.to_datetime(str(year_start-1)+"-01-01")

        year_start = pd.to_datetime(str(year_start)+"-01-01")

        # if year_start > pd.to_datetime(self.reporting_date):
        #     ValueError(f"Reporting date is earlier than the starting date: {self.reporting_date}!!!")
        #     return 0
        
        year_start = year_start.strftime("%Y-%m-%d")

        VWA = self._calculate_price(year_start,today,linear_average)

        if verbose:
            print(f'YTD weighted average Wholesale Electricity Prices for \
                  {pd.to_datetime(year_start).strftime("%B %d %Y")} to {pd.to_datetime(self.reporting_date).strftime("%B %d %Y")}')
            print(VWA.round(2))

        return VWA.round(2)
    
    def last_x_days(self, today='', year=2024, days=7, linear_average=False, verbose=False):
        if not today:
            today = self._get_offset_date(self.today,year)
        else:
            today = self._get_offset_date(today,year)

        self.reporting_date = self._report_date(today)
        
        past_date = today - pd.Timedelta(days=days)
        past_date = past_date.strftime('%Y-%m-%d')

        today = today.strftime("%Y-%m-%d")

        price = self.price.query(f"index >= '{past_date} 00:05' \
                        and index <= '{today} 00:00'") # FILTER DATES

        if linear_average:
            demand = self._lin_average(past_date,today)
        else:
            demand = self.demand.query(f"index >= '{past_date} 00:05' \
                                and index <= '{today} 00:00'").copy() # FILTER DATES
            
        if self.isNem:
            VWA = demand*price # VOLUME WEIGHTIFY THE REGIONAL PRICES WITH DEMAND (MW)
            
            demand['NEM'] = demand.sum(axis=1) # Total NEM demand
            VWA['NEM'] = VWA.sum(axis=1) # Volume weighted NEM prices
        else:
            VWA = pd.DataFrame({"Other":price.Price*demand.Demand})
            demand.columns = ["Other"]
        
        # prepare dfs for resampling
        if self.isNem:
            VWA.index = VWA.index - pd.Timedelta(minutes=5)
            demand.index = demand.index - pd.Timedelta(minutes=5)
        else:
            VWA.index = VWA.index - pd.Timedelta(minutes=30)
            demand.index = demand.index - pd.Timedelta(minutes=30)

        VWA = VWA.resample('1d').sum()
        demand = demand.resample('1d').sum()
        VWA = VWA / demand

        VWA_transposed = VWA.transpose()

        if verbose:
            print(f'Volume Weighted Average Wholesale Electricity Prices for the Past {str(days)} Days \
                   Up To Today: {pd.to_datetime(self.reporting_date).strftime("%B %d %Y")}')
            print(VWA_transposed.round(2))

        return VWA_transposed.round(2)

    def summarise(self,linear_average=False,verbose=False):
        '''
        INPUT
        prices: must be a class
        '''
        thisyear = pd.to_datetime(self.yesterday).year
        lastyear = pd.to_datetime(self.yesterday).year-1
        n364days = pd.to_datetime(self.today)-pd.Timedelta(days=364)
        
        if self.isDKIS:

            date = pd.to_datetime(self.last_dkis_day).strftime("%d %b %Y")

            dfs = [ self.latest_available(today=date,linear_average=linear_average,verbose=verbose),
                    self.full_qt(today=date,year=lastyear,linear_average=linear_average,verbose=verbose),
                    self.qtd(today=date,year=thisyear,linear_average=linear_average,verbose=verbose),
                    self.ytd(today=date,year_start=lastyear,linear_average=linear_average,verbose=verbose),
                    self.ytd(today=date,year_start=thisyear,linear_average=linear_average,verbose=verbose),
                    self.last_x_days(today=date,year=thisyear,linear_average=linear_average,verbose=verbose),
                    self.peak_demand ]

        else:
            date = pd.to_datetime(self.today).strftime("%d %b %Y")

            dfs = [ self.latest_available(linear_average=linear_average,verbose=verbose),
                    self.full_qt(year=lastyear,linear_average=linear_average,verbose=verbose),
                    self.qtd(year=thisyear,linear_average=linear_average,verbose=verbose),
                    self.ytd(year_start=lastyear,linear_average=linear_average,verbose=verbose),
                    self.ytd(year_start=thisyear,linear_average=linear_average,verbose=verbose),
                    self.last_x_days(year=thisyear,linear_average=linear_average,verbose=verbose),
                    self.peak_demand ]
        
        if self.isNem:
            lastmonth = pd.to_datetime(self.today)-pd.offsets.DateOffset(months=1)

            current_period = pd.to_datetime(self.today).to_period("Q")
            onequarterago = current_period.start_time.strftime("%Y-%m-%d")
            twoquartersago = (current_period-1).start_time.strftime("%Y-%m-%d")

            dfs = dfs + [self.mtd(year=thisyear,linear_average=linear_average,verbose=verbose),
                self.full_month(year=lastyear,linear_average=linear_average,verbose=verbose),
                self.full_month(today=lastmonth.strftime("%Y-%m-%d"),year=thisyear,
                                linear_average=linear_average,verbose=verbose),
                self.full_month(today=lastmonth.strftime("%Y-%m-%d"),year=lastyear,
                                linear_average=linear_average,verbose=verbose),
                self.fixed_rolling_average(start=self.yesterday,end=self.today,year=thisyear),
                self.fixed_rolling_average(start=(n364days-pd.Timedelta(days=1)).strftime("%Y-%m-%d"),
                                           end=n364days.strftime("%Y-%m-%d")),
                self.full_qt(today=onequarterago,year=thisyear,linear_average=linear_average,verbose=verbose),
                self.full_qt(today=onequarterago,year=lastyear,linear_average=linear_average,verbose=verbose),
                self.full_qt(today=twoquartersago,year=thisyear,linear_average=linear_average,verbose=verbose),
                self.ytd(today=f"{thisyear}-01-01",linear_average=linear_average,verbose=verbose)
            ]

        df = pd.concat(dfs,ignore_index=True,axis=1)

        qt = (4,1,2,3,4,4)
        
        if self.isNem:
            df.columns = [
            date,
            f"Q{self.quarter} - {lastyear}",
            f"Q{self.quarter} - {thisyear}~^",
            f"YTD {lastyear}",
            f"YTD {thisyear}",
            "-6","-5","-4","-3","-2","-1","Today","Peak Demand",
            f"MTD {thisyear}",
            f"Full Month {lastyear}",
            f"Full Previous Month {thisyear}",
            f"Full Previous Month {lastyear}",
            f"28 Day Average {thisyear}",
            f"28 Day Average {lastyear}",
            f"Q{qt[self.quarter-1]} {thisyear}",
            f"Q{qt[self.quarter-1]} {lastyear}",
            f"Q{qt[self.quarter-2]} {lastyear}",
            f"{lastyear} CY Average"
            ]

            new_order = [0,1,2,4,3,5]  # New order of rows
            df = df.take(new_order)

        else:
            df.columns = [
            date,
            f"Q{self.quarter} - {lastyear}",
            f"Q{self.quarter} - {thisyear}~^",
            f"YTD {lastyear}",
            f"YTD {thisyear}",
            "-6","-5","-4","-3","-2","-1","Today","Peak Demand"]
        
        return df.copy()
    

### DETECT IF PREVIOUS PRICE WAS LOWER OR HIGHER AND IF IT CHANGED - AND FEED THAT OUTPUT INTO MAIN
    
# prices = wholesale_electricy_price("./data/NEM_Prices.csv",nem=True,wem=False)
# prices = wholesale_electricy_price("./data/WEM_Prices.csv",wem=True,nem=False)
# prices = wholesale_electricy_price("./data/DKIS_Prices.csv",wem=False,nem=False)

# print(prices._calculate_price(start='2023-01-01',end='2023-10-01',linear_average=False))
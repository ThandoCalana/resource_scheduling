import os
import argparse
from datetime import date
from dotenv import load_dotenv
import snowflake.connector
from snowflake.connector.pandas_tools import write_pandas

load_dotenv()

import pandas as pd
import numpy as np

def create_dim_date():
    result_list = []
    
    # Edit values to specify range for DIM_DATE
    for year in range(2026,2028):
        for month in range(1,13):
            for day in range(1,32):
                try:
                    result = pd.to_datetime(f'{year}-{month}-{day}')
                    date = result.strftime('%d/%m/%Y')
                    full_year = result.strftime('%Y')
                    shortyear = result.strftime('%y')
                    monthname = result.strftime('%B')
                    monthabbrv = result.strftime('%b').title()
                    formatmonth = result.strftime('%m')
                    dayabbrv = result.strftime('%a')
                    weekday = result.strftime('%A')
                    monthday = result.strftime('%d')
                    if date[0:5] == "01/01":
                        public_holiday = "New Year's Day"
                    elif date[0:5] == "21/03":
                        public_holiday = "Human Rights Day"
                    elif int(monthday) <= 7 and weekday == "Friday" and monthname == "April":
                        public_holiday = "Good Friday"
                    elif 2 < int(monthday) <= 9 and weekday == "Sunday" and monthname == "April":
                        public_holiday = "Easter Sunday"
                    elif 3 < int(monthday) <= 10 and weekday == "Monday" and monthname == "April":
                        public_holiday = "Family Day"
                    elif date[0:5] == "27/04":
                        public_holiday = "Freedom Day"
                    elif date[0:5] == "01/05":
                        public_holiday = "Worker's Day"
                    elif date[0:5] == "16/06":
                        public_holiday = "Youth Day"
                    elif date[0:5] == "09/08":
                        public_holiday = "National Women's Day"
                    elif date[0:5] == "24/09":
                        public_holiday = "Heritage Day"
                    elif date[0:5] == "16/12":
                        public_holiday = "Day of Reconciliation"
                    elif date[0:5] == "25/12":
                        public_holiday = "Christmas Day"
                    elif date[0:5] == "26/12":
                        public_holiday = "Day of Goodwill"
                    else:
                        public_holiday = None
                    holiday_flag = True if public_holiday else False
                    result_list.append( [result, date, full_year, shortyear, monthname, monthabbrv, formatmonth, weekday,
                                        dayabbrv, monthday, str(public_holiday), str(holiday_flag)] )
                except ValueError:
                    continue

    df = pd.DataFrame(result_list, columns=["FULL_DATE", "DATE", "FULL_YEAR", "SHORT_YEAR", "MONTH_NAME", 
                                            "MONTH_ABBRV", "SHORT_MONTH", "WEEKDAY", "DAY_ABBRV",
                                            "DAY_OF_MONTH", "PUBLIC_HOLIDAY", "HOLIDAY_FLAG"])
    
    return df

def load_to_snowflake(dim_date:pd.DataFrame):

    conn = snowflake.connector.connect(
        account   = os.environ["SNOWFLAKE_ACCOUNT"],
        user      = os.environ["SNOWFLAKE_USER"],
        password  = os.environ["SNOWFLAKE_PASSWORD"],
        warehouse = os.environ["SNOWFLAKE_WAREHOUSE"],
        database  = os.environ["SNOWFLAKE_SCH_DB"],
        schema    = os.environ["SNOWFLAKE_SCH_SCHEMA"],
    )
    cur = conn.cursor()

    cur.execute("TRUNCATE TABLE DIM_DATE")

    write_pandas(
        conn, dim_date, "DIM_DATE",
        on_error="continue",
        auto_create_table=False,use_logical_type=True
    )


    cur.close()
    conn.close()


if __name__ == "__main__":
    dim_date = create_dim_date()
    load_to_snowflake(dim_date)
    print("DIM_DATE updated")

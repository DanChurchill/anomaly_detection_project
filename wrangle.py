from itertools import combinations_with_replacement
import numpy as np
import pandas as pd
import env
import warnings
warnings.filterwarnings("ignore")


def get_connection(db, user=env.user, host=env.host, password=env.password):
        return f'mysql+pymysql://{user}:{password}@{host}/{db}'


def get_cohort_dates():
    df = pd.read_sql('''            
                            SELECT id as cohort, start_date, end_date, program_id 
                            FROM curriculum_logs.cohorts;  ''', get_connection('curriculum_logs'))
    return df


# Import .txt file and convert it to a DataFrame object
def get_access_logs():
    df = pd.read_table("anonymized-curriculum-access.txt", sep = '\s', header = None,
                    names = ['date', 'time', 'page', 'id', 'cohort', 'ip'])
    return df

def combine_logs(df1,df2):
    logs = pd.merge(df1, df2, how='left', left_on='cohort', right_on='cohort')
    logs.date = pd.to_datetime(logs.date)
    logs.start_date = pd.to_datetime(logs.start_date)
    logs.end_date = pd.to_datetime(logs.end_date)    
    logs['active'] = np.where((logs['date'] >= logs.start_date) & (logs['date'] <= logs.end_date), 1, 0)
    return logs

def wrangle_logs():
    df1 = get_access_logs()
    df2 = get_cohort_dates()
    logs = combine_logs(df1,df2)
    return logs



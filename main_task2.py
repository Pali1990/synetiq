import pandas as pd
import os
import glob
import sqlite3
import numpy as np

def read_summarized_file(current_dir,pattern):
    files = [ i for i in  glob.glob(current_dir+"\\"+pattern)]
    return pd.read_csv(files[0],sep=',')


def join(df1,df2):
    conn = sqlite3.connect(':memory:')
    df1.to_sql('hr_df', conn, index=False)
    df2.to_sql('stimulus_df', conn, index=False)
    query = '''
    SELECT unix_utc,
        h.IBT,
        h.user_id,
        h.test_id,
        s.stimulus,
        s.diff_seconds as duration_secs
    FROM
        hr_df as h  JOIN stimulus_df as s
             ON
                h.user_id = s.user_id AND h.test_id = s.test_id 
             AND h.unix_utc between s.server_time and s.server_time_end
    WHERE s.diff_seconds > 15.00
    '''
    df = pd.read_sql_query(query, conn)
    return df


def RMSSD(s):
    return np.sqrt(np.mean(np.square(np.diff(s))))

def rmssd_score(input_df):
    df = input_df[['unix_utc','stimulus','user_id','test_id','IBT']]
    df['user_test'] = df['user_id'].astype(str) +'_'+ df['test_id'].astype(str)
    df.drop(columns = ['user_id','test_id'],axis=1,inplace = True)
    df = df.groupby(['stimulus','user_test'])['IBT'].apply(RMSSD).unstack()
    df['mean'] = df.mean(axis=1)
    return df.to_csv('task2_final.csv',float_format='%.4f')


if __name__ == "__main__":
    all_hr = read_summarized_file(os.getcwd(),"all_hr_gsr.csv")
    all_stimulus = read_summarized_file(os.getcwd(),"all_stimulus.csv")
    merged_df = join(all_hr,all_stimulus)
    rmssd_score(merged_df)

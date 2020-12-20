import os 
import glob
import pandas as pd
import numpy as np
import datetime
from concurrent.futures import ThreadPoolExecutor, ProcessPoolExecutor


def read_certain_files(current_dir,pattern):
    files = [ i for i in  glob.glob(current_dir+"\\data\\*\\*\\"+pattern+"\\*.csv", recursive = True)]
    return files

def parralel_process(functions,files):
    with ThreadPoolExecutor(8) as pool:
        df = pd.concat(pool.map(functions, files))
    return df


def utcdateparse (time_in_secs):
    return datetime.datetime.fromtimestamp(float(time_in_secs))


def read_hr_file(file):
    test_id = int((file.split("\\")[-3]))
    user_id = int(file.split("\\")[-4])
    df = pd.read_csv(file,sep=',', parse_dates=[0],date_parser=utcdateparse).assign(user_id=user_id).assign(test_id=test_id)
    return df


def cet_converter (ts_cet):
    return pd.DatetimeIndex(ts_cet).tz_convert('Europe/Budapest')

def read_stimulus_file(file):
    test_id = int((file.split("\\")[-3]))
    user_id = int(file.split("\\")[-4])
    df = pd.read_csv(file,sep=',',parse_dates= ['server_time'], date_parser=cet_converter).assign(user_id=user_id).assign(test_id=test_id)
    df['server_time'] = pd.to_datetime(df['server_time'], utc = False).dt.tz_localize(None)
    df['stimulus_end'] = df['stimulus'].shift(-1)
    df['server_time_end'] = df['server_time'].shift(-1)
    df = df[~df.stimulus.str.contains("_end")]
    df['diff_seconds'] = df['server_time_end'] - df['server_time']
    df['diff_seconds']=df['diff_seconds']/np.timedelta64(1,'s')
    return df



if __name__ == "__main__":
    hr_files = read_certain_files(os.getcwd(),"hr")
    stimulus_files = read_certain_files(os.getcwd(),"stimulus")
    hr = parralel_process(read_hr_file,hr_files)
    stimulus = parralel_process(read_stimulus_file,stimulus_files)
    hr.to_csv('all_hr_gsr.csv',sep=',',index=False)
    stimulus.to_csv('all_stimulus.csv',sep=',',index=False)

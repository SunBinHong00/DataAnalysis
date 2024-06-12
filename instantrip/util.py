import pandas as pd
import numpy as np

def pre_time(df,sincos=False):
    df = df.copy()
    # jt 비행시간 350은 03시간50분으로 230분으로 변환
    df['departure_jt'] = df['departure_jt'].apply(lambda x : str(x).zfill(4)).apply(lambda x : int(x[:2])*60+int(x[2:]))
    df['arrival_jt'] = df['arrival_jt'].apply(lambda x : str(x).zfill(4)).apply(lambda x : int(x[:2])*60+int(x[2:]))

    # str to datetime 형식 변환
    df['departure_sdt'] = pd.to_datetime(df['departure_sdt'])
    df['arrival_sdt'] = pd.to_datetime(df['arrival_sdt'])

    # 시*60 + 분
    df['departure_time'] = df['departure_sdt'].apply(lambda x: x.hour * 60 + x.minute)
    df['arrival_time'] = df['arrival_sdt'].apply(lambda x: x.hour * 60 + x.minute)
    
    if sincos:
        # 출발 도착 시각(하루 1440분 기준) sin cos 변환
        df['departure_time_sin'] = np.sin(2 * np.pi * df['departure_time'] / 1440)
        df['departure_time_cos'] = np.cos(2 * np.pi * df['departure_time'] / 1440)
        df['arrival_time_sin'] = np.sin(2 * np.pi * df['arrival_time'] / 1440)
        df['arrival_time_cos'] = np.cos(2 * np.pi * df['arrival_time'] / 1440)

        # 출발 도착 요일 sin cos 변환
        df['dep_week_sin'] = np.sin(2 * np.pi * df['dep_week'] / 7)
        df['dep_week_cos'] = np.cos(2 * np.pi * df['dep_week'] / 7)
        df['arr_week_sin'] = np.sin(2 * np.pi * df['arr_week'] / 7)
        df['arr_week_cos'] = np.cos(2 * np.pi * df['arr_week'] / 7)
        drop_col = ['departure_sdt','arrival_sdt',
                    'departure_hour','departure_minute','arrival_hour','arrival_minute',
                    'dep_week', 'arr_week']
        df.drop(columns=drop_col,inplace=True)
    else:
        drop_col = ['departure_sdt','arrival_sdt']
        df.drop(columns=drop_col,inplace=True)
    return df
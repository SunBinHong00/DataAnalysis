import pandas as pd
import numpy as np

def pre_time(df,sincos=False):
    df = df.copy()
    # jt 비행시간 350은 03시간50분으로 230분으로 변환
    df['dep_jt'] = df['departure_jt'].apply(lambda x : str(x).zfill(4)).apply(lambda x : int(x[:2])*60+int(x[2:]))
    df['arr_jt'] = df['arrival_jt'].apply(lambda x : str(x).zfill(4)).apply(lambda x : int(x[:2])*60+int(x[2:]))

    # str to datetime 형식 변환
    df['dep_sdt'] = pd.to_datetime(df['departure_sdt'])
    df['arr_sdt'] = pd.to_datetime(df['arrival_sdt'])

    # 시*60 + 분
    df['dep_time'] = df['dep_sdt'].apply(lambda x: x.hour * 60 + x.minute)
    df['arr_time'] = df['arr_sdt'].apply(lambda x: x.hour * 60 + x.minute)
    
    if sincos:
        # 출발 도착 시각(하루 1440분 기준) sin cos 변환
        df['dep_time_sin'] = np.sin(2 * np.pi * df['dep_time'] / 1440)
        df['dep_time_cos'] = np.cos(2 * np.pi * df['dep_time'] / 1440)
        df['arr_time_sin'] = np.sin(2 * np.pi * df['arr_time'] / 1440)
        df['arr_time_cos'] = np.cos(2 * np.pi * df['arr_time'] / 1440)

        # 출발 도착 요일 sin cos 변환
        df['dep_week_sin'] = np.sin(2 * np.pi * df['dep_week'] / 7)
        df['dep_week_cos'] = np.cos(2 * np.pi * df['dep_week'] / 7)
        df['arr_week_sin'] = np.sin(2 * np.pi * df['arr_week'] / 7)
        df['arr_week_cos'] = np.cos(2 * np.pi * df['arr_week'] / 7)
        drop_col = ['dep_sdt','arr_sdt',
                    'departure_hour','departure_minute',
                    'arrival_hour','arrival_minute',
                    'dep_week', 'arr_week']
        df.drop(columns=drop_col,inplace=True)
    else:
        drop_col = ['dep_sdt','arr_sdt',
                    'departure_jt','arrival_jt',
                    'departure_sdt','arrival_sdt']
        df.drop(columns=drop_col,inplace=True)
        new_order = [
                    'nights',
                    'dep_week','dep_time','dep_jt',
                    'arr_week','arr_time','arr_jt',
                    'partition_0','partition_1','time_zone',
                    'agentcode','baggagetype',
                    'total_fare'
                    ]
        df = df[new_order]
        
    return df
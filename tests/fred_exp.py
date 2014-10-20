from fredapi import Fred
fred = Fred(api_key='c28c8c84d9d04f5197a1896799baccf3')
data = fred.get1d(series_id='ACOILWTICO', observation_start='2012-01-01', observation_end='2014-10-01', frequency='d')
for i in data:
    print(i)

import pandas as pd
def sp500():
    table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    return list(df["Symbol"].str.replace('.', '-'))

def custom(file):
    f = open(file, "r")
    l = f.read().splitlines()
    return l

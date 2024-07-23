import pandas as pd
def sp500():
    table=pd.read_html('https://en.wikipedia.org/wiki/List_of_S%26P_500_companies')
    df = table[0]
    return list(df["Symbol"].str.replace('.', '-', regex=False))

def russel3000(format = 'list'):
    table = pd.read_csv('https://www.ishares.com/us/products/239714/ishares-russell-3000-etf/1467271812596.ajax?fileType=csv&fileName=IWV_holdings&dataType=fund', header=9, index_col=False)
    table = table[table.Ticker.str.match('^[A-Z]*$')]
    table = table[~table["Exchange"].str.contains("NO MARKET")]
    table["Ticker"] = table["Ticker"].str.replace('BRKB', 'BRK-B', regex=False)
    table["Ticker"] = table["Ticker"].str.replace('^BFB$', 'BF-B', regex=True)
    table["Ticker"] = table["Ticker"].str.replace('^BFA$', 'BF-A', regex=True)
    if format == 'list':
        return list(table["Ticker"])
    else:
        table.insert(0, 'Ranking', table.index)
        return table

def custom(file):
    f = open(file, "r")
    l = f.read().splitlines()
    return l

class Transaction():
    def __init__(self, buy_date, buy_price, sell_date=None, sell_price=None):
        self.buy_date = buy_date
        self.sell_date = sell_date
        self.buy_price = buy_price
        self.sell_price = sell_price

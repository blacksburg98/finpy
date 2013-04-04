class Order():
    def __init__(self, action, shares, tick, date, price=None):
        """
        If price is None, we will use adjusted price of th date.
        We can only initialize the price when we instantiate Portfolio.
        """
        self.action = action
        self.shares = shares
        self.tick = tick 
        self.price = price
        self.date = date


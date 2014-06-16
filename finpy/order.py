"""
(c) 2013 Tsung-Han Yang
This source code is released under the Apache license.  
blacksburg98@yahoo.com
Created on April 1, 2013
"""

class Order():
    def __init__(self, action, shares, tick, date, price=None):
        """
        If price is None, we will use adjusted price of the date.
        We can only initialize the price when we instantiate Portfolio.
        """
        self.action = action
        self.shares = shares
        self.tick = tick 
        self.price = price
        self.date = date


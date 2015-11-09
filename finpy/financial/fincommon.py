"""
(c) 2013 Tsung-Han Yang
This source code is released under the Apache license.  
blacksburg98@yahoo.com
Created on April 1, 2013
"""
import numpy as np
from finpy.utils import utils as ut
class FinCommon():
    """
    This class has some common functions used by both Equity and Portfolio.
    This is an abstract class.
    """
    def avg_daily_return(self):
        """ 
        Average of the daily_return list 
            :return np.average(self.daily_return()):
        """
        return np.average(self.daily_return())

    def beta_alpha(self, benchmark):
        """
        benchmark is an Equity representing the market. 
        It can be S&P 500, Russel 2000, or your choice of market indicator.
        This function uses polyfit in numpy to find the closest linear equation.
            :return beta:
            :return alpha:
        """
        beta, alpha = np.polyfit(benchmark.normalized(), self.normalized(), 1)
        return beta, alpha

    def beta(self, benchmark):
        """
        benchmark is an Equity representing the market. 
        This function uses cov in numpy to calculate beta.
        """
        benchmark_close = benchmark.normalized() 
        C = np.cov(benchmark_close, self.normalized())/np.var(benchmark_close)
        beta = C[0][1]/C[0][0]
        return beta

    def sharpe_ratio(self, rf_tick="$TNX"):
        """
        Return the Original Sharpe Ratio.
        https://en.wikipedia.org/wiki/Sharpe_ratio
        rf_tick is Ten-Year treasury rate ticker at Yahoo.

        """
        return self.mean_excess_return(rf_tick)/self.excess_risk(rf_tick)

    def info_ratio(self, benchmark, rf_tick="$TNX"):
        """
        Information Ratio
        https://en.wikipedia.org/wiki/Information_ratio
        Information Ratio is defined as active return divided by active risk,
        where active return is the difference between the return of the security
        and the return of a selected benchmark index, and active risk is the
        standard deviation of the active return.
        """
        return self.mean_active_return(benchmark)/self.active_risk(benchmark)

    def appraisal_ratio(self, benchmark, rf_tick="$TNX"):
        """
        Appraisal Ratio
        https://en.wikipedia.org/wiki/Appraisal_ratio
        Appraisal Ratio is defined as residual return divided by residual risk,
        where residual return is the difference between the return of the security
        and the return of a selected benchmark index, and residual risk is the
        standard deviation of the residual return.
        """
        return self.mean_residual_return(benchmark, rf_tick)/self.residual_risk(benchmark, rf_tick)

    def excess_return(self, rf_tick="$TNX"):
        """
        An active return is the difference between the benchmark and the actual return.
        """
        return self.daily_return() - ut.riskfree_return(self.ldt_timestamps(), rf_tick="$TNX")

    def mean_excess_return(self, rf_tick="$TNX"):
        return np.mean(self.excess_return(rf_tick))

    def excess_risk(self, rf_tick="$TNX"):
        """
        $FVX is another option. Five-Year treasury rate.
        An excess risk is the standard deviation of the excess return.
        """
        return np.std(self.excess_return(rf_tick))

    def active_return(self, benchmark):
        """
        An active return is the difference between the benchmark and the actual return.
        """
        return self.daily_return() - benchmark.daily_return()

    def mean_active_return(self, benchmark):
        return np.mean(self.active_return(benchmark))

    def active_risk(self, benchmark):
        """
        An active risk is the standard deviation of the active return.
        """
        return np.std(self.active_return(benchmark))

    def residual_return(self, benchmark, rf_tick="$TNX"):
        """
        A residual return is the excess return minus beta times the benchmark excess return.
        """
        beta = self.beta(benchmark)
        return  self.excess_return(rf_tick="$TNX") - beta * benchmark.excess_return(rf_tick="$TNX")

    def residual_risk(self, benchmark, rf_tick="$TNX"):
        """
        Residual Risk is the standard deviation of the residual return.
        """
        return np.std(self.residual_return(benchmark, rf_tick))

    def mean_residual_return(self, benchmark, rf_tick="$TNX"):
        return np.mean(self.residual_return(benchmark, rf_tick))


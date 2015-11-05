from finpy.sim.sim import Sim
if __name__ == '__main__':
    sim = Sim()
    stat, div, divstd, pie, summary = sim.run_algo()
    output, fund_graph = sim.backtesting()
    sim.sim_output(output, fund_graph, stat, div, divstd, pie, summary)

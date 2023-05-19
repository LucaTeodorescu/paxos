from paxos.basic_protocol import *
from datetime import timedelta
import numpy as np
import time
import matplotlib.pyplot as plt
import pandas as pd
import seaborn as sns

NB_SIMULATION = 10
FAILURE_RATE_RANGE = [0, 0.02, 0.04, 0.06, 0.08, 0.10]
NB_ACCEPTORS = 5
NB_PROPOSERS = 1
precomputed = False


class Simulation:
    # Class simulation to run a simulation

    def __init__(self, 
                 n_proposers: int, 
                 n_acceptors: int,
                 messenger_failure_rate = 0, 
                 proposer_fail_rate=0, 
                 messenger_max_delay=0.5
                 ):
        self.messenger = UnreliableMessenger(failure_rate=messenger_failure_rate, 
                                             max_delay=messenger_max_delay)
        self.assembly = Assembly(n_proposers=n_proposers, 
                                 n_acceptors=n_acceptors, 
                                 messenger=self.messenger, 
                                 proposer_fail_rate=proposer_fail_rate, 
                                 period_proposer=timedelta(seconds=messenger_max_delay*18 + 5)) # The 18 from page 13-14 of Leslie Lamport Part-Time Parliament paper

    def start(self):
        start_time = time.time()
        result = self.assembly.start()
        print(result)
        time_measured = time.time() - start_time
        return time_measured


if __name__ == '__main__':
    if precomputed:
        df_TIMES = pd.read_csv('experiments/failure_rate.csv')
    else:
        TIMES = []
        for failure_rate in FAILURE_RATE_RANGE:
            RESULTS = []
            for i in range(NB_SIMULATION):
                simul = Simulation(n_proposers=NB_PROPOSERS, n_acceptors=NB_ACCEPTORS, messenger_failure_rate = failure_rate, proposer_fail_rate=0, messenger_max_delay=0)
                RESULTS.append(simul.start())
            TIMES.append(np.mean(RESULTS))
        df_TIMES = pd.DataFrame(data=TIMES)
        df_TIMES.to_csv('experiments/failure_rate.csv')
    fig, ax = plt.subplots(figsize=(6, 6))
    plt.plot(FAILURE_RATE_RANGE, TIMES)
    plt.ylabel("Seconds")
    plt.xlabel("Failure Rate")
    ax.set_title('Average time needed to achieve a consensus (seconds)')
    fig.savefig('experiments/failure_rate.png')
    


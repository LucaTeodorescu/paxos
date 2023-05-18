from paxos.basic_protocol import *
import numpy as np
import time
import matplotlib.pyplot as plt

NB_SIMULATION = 10
FAILURE_RATE_RANGE = [0, 0.01, 0.02, 0.03, 0.04, 0.05, 0.06, 0.07, 0.08, 0.09, 0.10]
NB_OF_ACCEPTOR = 5
NB_PROPOSERS = 1


class Simulation:
    # Class simulation to run a simulation

    def __init__(self, 
                 n_proposers: int, 
                 n_acceptors: int,
                 messenger_failure_rate = 0, 
                 proposer_fail_rate=0, 
                 messenger_max_delay=0.5
                 ):
        self.messenger = UnreliableMessenger(failure_rate=messenger_failure_rate, max_delay=messenger_max_delay)
        self.assembly = Assembly(n_proposers=n_proposers, n_acceptors=n_acceptors, messenger=self.messenger, proposer_fail_rate=proposer_fail_rate)

    def start(self):
        start_time = time.time()
        result = self.assembly.start()
        print(result)
        time_measured = time.time() - start_time
        return time_measured


if __name__ == '__main__':
    list_time = []
    for failure_rate in FAILURE_RATE_RANGE:
        results_list = []
        for i in range(NB_SIMULATION):
            simul = Simulation(n_proposers=NB_PROPOSERS, n_acceptors=NB_OF_ACCEPTOR, messenger_failure_rate = failure_rate, proposer_fail_rate=0, messenger_max_delay=0)
            results_list.append(simul.start())
        list_time.append(np.mean(results_list))
    plt.plot(list_time)
    plt.ylabel('time in seaconds')


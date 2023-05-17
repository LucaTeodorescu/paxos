from paxos.basic_protocol import *
import numpy as np
import time
import matplotlib.pyplot as plt


class Simulation:
    # Class simulation to run a simulation

    def __init__(self, 
                 n_proposers: int, 
                 n_acceptors: int,
                 messenger_failure_rate = 0, 
                 proposer_fail_rate=0, 
                 messenger_avg_delay=1
                 ):
        self.messenger = UnreliableMessenger(failure_rate=messenger_failure_rate, avg_delay=messenger_avg_delay)
        self.assembly = Assembly(n_proposers=n_proposers, n_acceptors=n_acceptors, messenger=self.messenger, proposer_fail_rate=proposer_fail_rate)

    def start(self):
        start_time = time.time()
        result = self.assembly.start()
        print(result)
        time_measured = time.time() - start_time
        return time_measured


if __name__ == '__main__':
    list_time = []
    for j in range(5):
        results_list = []
        for i in range(1):
            simul = Simulation(n_proposers=1, n_acceptors=4, messenger_failure_rate = 0.01*j, proposer_fail_rate=0, messenger_avg_delay=0)
            results_list.append(simul.start())
        list_time.append(np.mean(results_list))
    plt.plot(list_time)
    plt.ylabel('time in seaconds')


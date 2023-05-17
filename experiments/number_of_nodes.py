import datetime
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from paxos.basic_protocol import ReliableMessenger, UnreliableMessenger, Assembly

NB_PROPOSERS = range(1, 8)
NB_ACCEPTORS = [1, 5, 10, 15, 20, 25]
NB_SIMULATIONS = 10


# No failure, no delay

def no_failure(precomputed=True):
    if precomputed:
        TIMES = pd.read_csv('number_of_nodes_no_failure.csv', index_col=0)
    else:
        TIMES = []
        for nb_prop in NB_PROPOSERS:
            TIMES.append([])
            for nb_acc in NB_ACCEPTORS:
                TIMES[-1].append(0)
                for _ in range(NB_SIMULATIONS):
                    print(f" === NEW INSTANCE OF THE ALGORITHM ({nb_prop} PROPOSERS, {nb_acc} ACCEPTORS) === ")
                    messenger = ReliableMessenger()
                    assembly = Assembly(n_proposers=nb_prop, n_acceptors=nb_acc, messenger=messenger)
                    for proposer in assembly.proposers:
                        proposer.period = datetime.timedelta(seconds=10)
                    begin = datetime.datetime.now()
                    result = assembly.start()
                    end = datetime.datetime.now()
                    TIMES[-1][-1] += (end - begin).total_seconds() / NB_SIMULATIONS

        pd.DataFrame(data=TIMES, columns=NB_ACCEPTORS, index=NB_PROPOSERS).to_csv('number_of_nodes_no_failure.csv')

    fig, ax = plt.subplots(figsize=(8, 8))
    sns.heatmap(data=TIMES, annot=True, fmt='.3g', cmap='seismic', square=True, ax=ax)
    ax.set_ylabel("Number of proposers")
    ax.set_yticklabels(NB_PROPOSERS)
    ax.invert_yaxis()
    ax.set_xlabel("Number of acceptors")
    ax.set_xticklabels(NB_ACCEPTORS)
    ax.set_title("Average time needed to reach consensus (in seconds)")
    fig.savefig('number_of_nodes_no_failure.png')


if __name__ == '__main__':
    no_failure(precomputed=True)

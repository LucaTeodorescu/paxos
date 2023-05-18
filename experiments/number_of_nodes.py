import datetime
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from paxos.basic_protocol import ReliableMessenger, UnreliableMessenger, Assembly


# No failure, no delay
def no_failure(precomputed=False):
    NB_PROPOSERS = range(1, 8)
    NB_ACCEPTORS = [1, 5, 10, 15, 20, 25]
    NB_SIMULATIONS = 10
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

    fig, ax = plt.subplots(figsize=(7, 7))
    sns.heatmap(data=TIMES, annot=True, fmt='.3g', cmap='seismic', square=True, ax=ax)
    ax.set_ylabel("Number of proposers")
    ax.set_yticklabels(NB_PROPOSERS)
    ax.invert_yaxis()
    ax.set_xlabel("Number of acceptors")
    ax.set_xticklabels(NB_ACCEPTORS)
    ax.set_title("Average time needed to reach consensus (seconds)")
    fig.savefig('number_of_nodes_no_failure.png')


# Failure from the acceptors only
def acceptor_failure(precomputed=False):
    NB_PROPOSERS = [1, 2, 3]
    NB_ACCEPTORS = [1, 3, 5, 7, 9, 11, 13, 15, 17, 19]
    NB_SIMULATIONS = 10
    if precomputed:
        TIMES = pd.read_csv('number_of_nodes_acceptor_failure.csv', index_col=0)
    else:
        TIMES = []
        for nb_prop in NB_PROPOSERS:
            TIMES.append([])
            for nb_acc in NB_ACCEPTORS:
                TIMES[-1].append(0)
                for _ in range(NB_SIMULATIONS):
                    print(f" === NEW INSTANCE OF THE ALGORITHM ({nb_prop} PROPOSERS, {nb_acc} ACCEPTORS) === ")
                    messenger = ReliableMessenger()
                    assembly = Assembly(
                        n_proposers=nb_prop, n_acceptors=nb_acc, messenger=messenger, acceptor_fail_rate=1e-7
                    )
                    for proposer in assembly.proposers:
                        proposer.period = datetime.timedelta(seconds=10)
                    begin = datetime.datetime.now()
                    result = assembly.start()
                    end = datetime.datetime.now()
                    TIMES[-1][-1] += (end - begin).total_seconds() / NB_SIMULATIONS

        TIMES = pd.DataFrame(data=TIMES, columns=NB_ACCEPTORS, index=NB_PROPOSERS)
        TIMES.to_csv('number_of_nodes_acceptor_failure.csv')

    TIMES['Proposers'] = TIMES.index.to_series()
    TIMES = pd.melt(TIMES, id_vars=['Proposers'], value_vars=map(str, NB_ACCEPTORS), var_name='Acceptors', value_name='Time')

    fig, ax = plt.subplots(figsize=(6, 6))
    sns.lineplot(data=TIMES, x='Acceptors', y='Time', hue='Proposers', ax=ax)
    ax.set_title('Average time needed to achieve a consensus (seconds)')
    fig.savefig('number_of_nodes_acceptor_failure.png')


# Failure from the proposers only
def proposer_failure(precomputed=False):
    NB_PROPOSERS = range(1, 6)
    NB_ACCEPTORS = [1, 5, 10, 15, 20]
    NB_SIMULATIONS = 10
    if precomputed:
        TIMES = pd.read_csv('number_of_nodes_proposer_failure.csv', index_col=0)
    else:
        TIMES = []
        for nb_prop in NB_PROPOSERS:
            TIMES.append([])
            for nb_acc in NB_ACCEPTORS:
                TIMES[-1].append(0)
                for _ in range(NB_SIMULATIONS):
                    print(f" === NEW INSTANCE OF THE ALGORITHM ({nb_prop} PROPOSERS, {nb_acc} ACCEPTORS) === ")
                    messenger = ReliableMessenger()
                    assembly = Assembly(
                        n_proposers=nb_prop, n_acceptors=nb_acc, messenger=messenger, proposer_fail_rate=1e-7
                    )
                    for proposer in assembly.proposers:
                        proposer.period = datetime.timedelta(seconds=10)
                    begin = datetime.datetime.now()
                    result = assembly.start()
                    end = datetime.datetime.now()
                    TIMES[-1][-1] += (end - begin).total_seconds() / NB_SIMULATIONS

        TIMES = pd.DataFrame(data=TIMES, columns=NB_ACCEPTORS, index=NB_PROPOSERS)
        TIMES.to_csv('number_of_nodes_acceptor_failure.csv')

    TIMES['Proposers'] = TIMES.index.to_series()
    TIMES = pd.melt(TIMES, id_vars=['Proposers'], value_vars=map(str, NB_ACCEPTORS), var_name='Acceptors', value_name='Time')

    fig, ax = plt.subplots(figsize=(6, 6))
    sns.lineplot(data=TIMES, x='Proposers', y='Time', hue='Acceptors', ax=ax)
    ax.set_title('Average time needed to achieve a consensus (seconds)')
    fig.savefig('number_of_nodes_proposer_failure.png')


if __name__ == '__main__':
    no_failure(precomputed=True)
    acceptor_failure(precomputed=True)
    proposer_failure(precomputed=True)

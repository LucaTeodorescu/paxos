import datetime
import pandas as pd
import seaborn as sns
import matplotlib.pyplot as plt

from paxos.multi_paxos import ReliableMessenger, UnreliableMessenger, Assembly


def experiment_multi_paxos(
        precomputed=False, nb_proposers=3, nb_acceptors=5,
        msg_fail_rate=.05, agent_fail_rate=1e-7,
        nb_simulations=10, max_instances=10
):
    if precomputed:
        data = pd.read_csv('multi_paxos.csv')
    else:
        times = [0]
        for n in range(1, max_instances + 1):
            times.append(0)
            for j in range(nb_simulations):
                print(f" === NEW INSTANCE OF THE ALGORITHM ({n} INSTANCES, {j+1}/{nb_simulations}) === ")
                messenger = UnreliableMessenger(failure_rate=msg_fail_rate, max_delay=0)
                assembly = Assembly(
                    n_proposers=nb_proposers, n_acceptors=nb_acceptors, messenger=messenger,
                    proposer_fail_rate=agent_fail_rate, acceptor_fail_rate=agent_fail_rate,
                    nb_instances=n, period_proposer=datetime.timedelta(seconds=10)
                )
                begin = datetime.datetime.now()
                result = assembly.start()
                end = datetime.datetime.now()
                times[-1] += (end - begin).total_seconds() / nb_simulations

        data = pd.DataFrame(columns=['Number of instances', 'Time'])
        data['Time'] = times
        data['Number of instances'] = range(max_instances + 1)
        data.to_csv('multi_paxos.csv', index=False)

    fig, ax = plt.subplots(figsize=(7, 4))
    sns.lineplot(data=data, x='Number of instances', y='Time', ax=ax)
    a = data.loc[1, 'Time']
    xs = [0, 10]
    ys = [0, 10*a]
    ax.plot(xs, ys, color='black', ls='dotted')
    ax.set_ylabel("Time")
    ax.set_xlabel("Number of instances")
    ax.set_title("Average time needed to reach consensus (seconds)")
    fig.savefig('multi_paxos.png')


if __name__ == '__main__':
    experiment_multi_paxos(precomputed=True)

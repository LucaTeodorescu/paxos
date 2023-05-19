from __future__ import annotations
from threading import Thread, Event
from datetime import timedelta, datetime
from typing import List, Optional
from numpy.random import choice

from paxos.base_classes import *


class Proposer(Agent):
    def __init__(self, messenger: Messenger, assembly: Assembly,
                 failure_rate: float = 0, avg_failure_duration: float = 5,
                 period: timedelta = timedelta(seconds=60)
                 ):
        super().__init__(messenger, failure_rate=failure_rate, avg_failure_duration=avg_failure_duration)
        self.period = period
        self.assembly = assembly
        self.last_tried: List[Optional[Ballot]] = [None for _ in range(self.assembly.nb_instances)]
        self.responses: List[List[Optional[Vote]]] = [list() for _ in range(self.assembly.nb_instances)]
        self.ledger: List[Optional[Ballot]] = [None for _ in range(self.assembly.nb_instances)]

    def start(self, event: Event) -> None:
        t0 = datetime.now() - self.period + timedelta(seconds=5)
        print(f"Agent #{self.id} started ({self.__class__.__name__})")
        self.messenger.register(self.id)
        while not event.is_set():
            # Process messages received
            message = self.messenger.get_message(self.id)
            if message is not None:
                self.process_message(message)
            # Regularly initiate ballots
            if datetime.now() - t0 >= self.period:
                t0 = datetime.now()
                self.initiate_new_ballot()
            # And maybe fail
            if random() < self.failure_rate:
                print(f"Agent #{self.id} failed ({self.__class__.__name__})")
                sleep(self.avg_failure_duration * exponential())
                break

        # If the agent failed, it starts again
        if not event.is_set():
            self.start(event)

    def process_message(self, message: Message):
        print(f"Agent #{self.id} received a {message.type} message from agent #{message.author_id}")
        if message.type is MessageType.LastVote:
            self.on_lastvote(message)
        elif message.type is MessageType.Voted:
            self.on_voted(message)
        elif message.type is MessageType.Success:
            self.on_success(message)

    def on_lastvote(self, message: Message):
        # The proposer checks whether the ballot number included in the message corresponds to
        #  a ballot that it is currently organizing
        idx = None
        for ballot in self.last_tried:
            if message.ballot_number == ballot.number:
                idx = self.last_tried.index(ballot)
        # If it is the case
        if idx is not None:
            self.responses[idx].append(message.last_vote)
            # If all responses are received
            if len(self.responses[idx]) == len(self.last_tried[idx].quorum):
                # Sets the decree to satisfy B3
                ballots = [v.ballot for v in self.responses[idx] if v is not None]
                ballot_numbers = [b.number for b in ballots]
                if ballots:
                    max_number = max(ballot_numbers)
                    max_ballot = [b for b in ballots if b.number == max_number][0]
                    self.last_tried[idx].decree = max_ballot.decree
                else:
                    self.last_tried[idx].decree = self.make_proposal()

                # Sends the BeginBallot message
                reponse = Message(
                    author_id=self.id,
                    type=MessageType.BeginBallot,
                    ballot=self.last_tried[idx],
                    decree=self.last_tried[idx].decree,
                )
                for acceptor in self.last_tried[idx].quorum:
                    self.messenger.send_message(acceptor.id, reponse)

    def on_voted(self, message: Message):
        # When the proposer receives a vote regarding its current ballot
        idx = None
        for ballot in self.last_tried:
            if message.vote.ballot.number == ballot.number:
                idx = self.last_tried.index(ballot)

        if idx is not None:
            # It adds one voter
            self.last_tried[idx].voters.add(message.vote.acceptor)
            # If the ballot becomes successful
            if self.last_tried[idx].successful:
                # It sends a message to the whole assembly
                response = Message(
                    author_id=self.id,
                    type=MessageType.Success,
                    decree=self.last_tried[idx].decree,
                    ballot_number=self.last_tried[idx].number
                )
                for agent in self.assembly.agents:
                    self.messenger.send_message(agent.id, response)

    def initiate_new_ballot(self):
        quorum = self.create_random_quorum()
        print(f'{self} selected the following quorum : {quorum}')

        for idx in range(self.assembly.nb_instances):
            # With this definition for ballot numbers, every ballot related to instance idx has a ballot_id such that
            #  ballot_id % nb_instances == idx
            if self.last_tried[idx] is None:
                b = BallotNumber(idx, self.id)
            else:
                b = BallotNumber(self.last_tried[idx].number.ballot_id + self.assembly.nb_instances, self.id)
            self.last_tried[idx] = Ballot(b, Proposal(None), quorum, set())
            self.responses[idx] = list()

            message = Message(
                author_id=self.id,
                type=MessageType.NextBallot,
                ballot_number=self.last_tried[idx].number,
            )
            for acceptor in self.last_tried[idx].quorum:
                self.messenger.send_message(acceptor.id, message)

    def create_random_quorum(self):
        m = len(self.assembly.acceptors) // 2 + 1
        return set(choice(list(self.assembly.acceptors), m, replace=False))

    def make_proposal(self):
        return Proposal(self.id)

    def on_success(self, message) -> None:
        print(f"Agent #{self.id} was notified that decree {message.decree} was accepted.")
        self.ledger[message.ballot_number.ballot_id % self.assembly.nb_instances] = message.decree


class Acceptor(Agent):
    def __init__(self, messenger: Messenger, assembly: Assembly,
                 failure_rate: float = 0, avg_failure_duration: float = 5,
                 ):
        super().__init__(messenger, failure_rate=failure_rate, avg_failure_duration=avg_failure_duration)
        self.assembly: Assembly = assembly
        self.last_vote: List[Optional[Vote]] = [None for _ in range(self.assembly.nb_instances)]
        self.next_ballot: List[Optional[BallotNumber]] = [None for _ in range(self.assembly.nb_instances)]
        self.ledger: List[Optional[Ballot]] = [None for _ in range(self.assembly.nb_instances)]

    def process_message(self, message: Message):
        print(f"Agent #{self.id} received a {message.type} message from agent #{message.author_id}")
        if message.type is MessageType.NextBallot:
            self.on_nextballot(message)
        elif message.type is MessageType.BeginBallot:
            self.on_beginballot(message)
        elif message.type is MessageType.Success:
            self.on_success(message)

    def on_nextballot(self, message: Message):
        # The acceptor first needs to find to which instance of the protocol the corresponding ballot number is related
        idx = message.ballot_number.ballot_id % self.assembly.nb_instances
        # Then it checks whether it should react to this number in this instance or not
        if self.next_ballot[idx] is None or message.ballot_number > self.next_ballot[idx]:
            self.next_ballot[idx] = message.ballot_number
            response = Message(
                author_id=self.id,
                type=MessageType.LastVote,
                ballot_number=message.ballot_number,
                last_vote=self.last_vote[idx]
            )
            self.messenger.send_message(message.author_id, response)

    def on_beginballot(self, message: Message):
        # The acceptor first needs to find to which instance of the protocol the corresponding ballot number is related
        idx = message.ballot.number.ballot_id % self.assembly.nb_instances
        # If the ballot is the one the acceptor is waiting for (in this instance)
        if message.ballot.number == self.next_ballot[idx]:
            # It votes for it
            self.last_vote[idx] = Vote(message.ballot, self)
            # And sends a Voted message to the proposer
            response = Message(author_id=self.id, type=MessageType.Voted, vote=self.last_vote[idx])
            self.messenger.send_message(message.author_id, response)

    def on_success(self, message) -> None:
        print(f"Agent #{self.id} was notified that decree {message.decree} was accepted.")
        self.ledger[message.ballot_number.ballot_id % self.assembly.nb_instances] = message.decree


class Assembly:
    def __init__(
            self,
            n_proposers: int,
            n_acceptors: int,
            nb_instances: int,
            messenger: Messenger = ReliableMessenger(),
            proposer_fail_rate: float = 0,
            acceptor_fail_rate: float = 0,
            period_proposer: timedelta = timedelta(seconds=60)
            ):
        self.nb_instances = nb_instances
        self.messenger = messenger
        self.proposers = {Proposer(self.messenger, self, failure_rate=proposer_fail_rate, period=period_proposer) for _ in range(n_proposers)}
        self.acceptors = {Acceptor(self.messenger, self, failure_rate=acceptor_fail_rate) for _ in range(n_acceptors)}
        self.threads = list()


    @property
    def agents(self):
        return self.proposers.union(self.acceptors)

    def start(self):
        end = Event()
        # We create a thread for the messenger if necessary
        if isinstance(self.messenger, UnreliableMessenger):
            # In this case, the messages are not delivered instantaneously
            # The messenger needs its own thread so that it does not block the agents
            self.threads.append(Thread(target=self.messenger.start, args=(end,), name="Messenger"))
            self.threads[-1].start()
        # And a thread for every agent
        for agent in self.agents:
            self.threads.append(Thread(target=agent.start, args=(end,), name=f'Agent #{agent.id}'))
            self.threads[-1].start()

        # We wait until a decision is made and all agents learn about it
        while None in [single_ledger for agent in self.agents for single_ledger in agent.ledger]:
            continue
        # When it is the case, we stop all the threads 
        end.set()

        ledgers_unique = []
        for agent in self.agents:
            if agent.ledger not in ledgers_unique:
                ledgers_unique.append(agent.ledger)
        assert len(ledgers_unique) == 1,\
            f"Failure : more than one proposal was accepted by a majority of voters ({ledgers_unique})"
        return ledgers_unique.pop()


if __name__ == '__main__':
    messenger = UnreliableMessenger(failure_rate=0.05, max_delay=5)
    assembly = Assembly(n_proposers=2, n_acceptors=5, nb_instances=2, messenger=messenger)
    result = assembly.start()
    print(result)

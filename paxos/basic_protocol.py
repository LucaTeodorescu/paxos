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
        # TODO: At which frequency should proposers initiate ballots?
        self.period = period
        self.assembly = assembly
        self.last_tried: Optional[Ballot] = None
        self.responses: List[Optional[Vote]] = list()

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
        if message.ballot_number == self.last_tried.number:
            self.responses.append(message.last_vote)
            # If all responses are received
            if len(self.responses) == len(self.last_tried.quorum):
                # Sets the decree to satisfy B3
                ballots = [v.ballot for v in self.responses if v is not None]
                ballot_numbers = [b.number for b in ballots]
                if ballots:
                    max_number = max(ballot_numbers)
                    max_ballot = [b for b in ballots if b.number == max_number][0]
                    self.last_tried.decree = max_ballot.decree
                else:
                    self.last_tried.decree = self.make_proposal()

                # Sends the BeginBallot message
                reponse = Message(
                    author_id=self.id,
                    type=MessageType.BeginBallot,
                    ballot=self.last_tried,
                    decree=self.last_tried.decree,
                )
                for acceptor in self.last_tried.quorum:
                    self.messenger.send_message(acceptor.id, reponse)

    def on_voted(self, message: Message):
        # When the proposer receives a vote regarding its current ballot
        if message.vote.ballot.number == self.last_tried.number:
            # It adds one voter
            self.last_tried.voters.add(message.vote.acceptor)
            # If the ballot becomes successful
            if self.last_tried.successful:
                # It sends a message to the whole assembly
                response = Message(author_id=self.id, type=MessageType.Success, decree=self.last_tried.decree)
                for agent in self.assembly.agents:
                    self.messenger.send_message(agent.id, response)

    def initiate_new_ballot(self):
        if self.last_tried is None:
            b = BallotNumber(0, self.id)
        else:
            b = BallotNumber(self.last_tried.number.ballot_id + 1, self.id)
        quorum = self.create_random_quorum()
        print(f'{self} selected the following quorum : {quorum}')
        self.last_tried = Ballot(b, Proposal(None), quorum, set())
        self.responses = list()

        message = Message(
            author_id=self.id,
            type=MessageType.NextBallot,
            ballot_number=self.last_tried.number,
        )
        for acceptor in self.last_tried.quorum:
            self.messenger.send_message(acceptor.id, message)

    def create_random_quorum(self):
        m = len(self.assembly.acceptors) // 2 + 1
        return set(choice(list(self.assembly.acceptors), m, replace=False))

    def make_proposal(self):
        return Proposal(self.id)


class Acceptor(Agent):
    def __init__(self, messenger: Messenger, assembly: Assembly,
                 failure_rate: float = 0, avg_failure_duration: float = 5,
                 ):
        super().__init__(messenger, failure_rate=failure_rate, avg_failure_duration=avg_failure_duration)
        self.assembly: Assembly = assembly
        self.last_vote: Optional[Vote] = None
        self.next_ballot: Optional[BallotNumber] = None

    def process_message(self, message: Message):
        print(f"Agent #{self.id} received a {message.type} message from agent #{message.author_id}")
        if message.type is MessageType.NextBallot:
            self.on_nextballot(message)
        elif message.type is MessageType.BeginBallot:
            self.on_beginballot(message)
        elif message.type is MessageType.Success:
            self.on_success(message)

    def on_nextballot(self, message: Message):
        if self.next_ballot is None or message.ballot_number > self.next_ballot:
            self.next_ballot = message.ballot_number
            response = Message(
                author_id=self.id,
                type=MessageType.LastVote,
                ballot_number=message.ballot_number,
                last_vote=self.last_vote
            )
            self.messenger.send_message(message.author_id, response)

    def on_beginballot(self, message: Message):
        if message.ballot.number == self.next_ballot:
            self.last_vote = Vote(message.ballot, self)
            # And sends a Voted message to the proposer
            response = Message(author_id=self.id, type=MessageType.Voted, vote=self.last_vote)
            self.messenger.send_message(message.author_id, response)


class Assembly:
    def __init__(
            self,
            n_proposers: int,
            n_acceptors: int,
            messenger: Messenger = ReliableMessenger(),
            proposer_fail_rate: float = 0,
            acceptor_fail_rate: float = 0
            ):
        self.messenger = messenger
        self.proposers = {Proposer(self.messenger, self, failure_rate=proposer_fail_rate) for _ in range(n_proposers)}
        self.acceptors = {Acceptor(self.messenger, self, failure_rate=acceptor_fail_rate) for _ in range(n_acceptors)}
        self.learners = {}  # TODO: Implement learners
        self.threads = list()

    @property
    def agents(self):
        return self.proposers.union(self.acceptors).union(self.learners)

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
        while None in [agent.ledger for agent in self.agents]:
            continue
        # When it is the case, we stop all the threads
        end.set()

        ledgers = set(agent.ledger.value for agent in self.agents)
        assert len(ledgers) == 1, f"Failure : more than one proposal was accepted by a majority of voters ({ledgers})"
        return ledgers.pop()


if __name__ == '__main__':
    messenger = UnreliableMessenger(failure_rate=0.05, avg_delay=1)
    assembly = Assembly(n_proposers=2, n_acceptors=5, messenger=messenger, proposer_fail_rate=1e-8)
    result = assembly.start()
    print(result)

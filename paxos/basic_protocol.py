from __future__ import annotations
from threading import Thread
from datetime import timedelta, datetime
from typing import List

from paxos.base_classes import *


class Proposer(Agent):
    def __init__(self, messenger: Messenger, assembly: Assembly, period: timedelta = timedelta(seconds=30)):
        super().__init__(messenger)
        # TODO: At which frequency should proposers initiate ballots?
        self.period = period
        self.assembly = assembly
        self.last_tried: Optional[Ballot] = None
        self.responses: List[Optional[Vote]] = list()

    def start(self):
        print(f"Agent #{self.id} started ({self.__class__.__name__})")
        # This definition for t0 allows the first ballot to be initiated 5 seconds after the Agent is started
        t0 = datetime.now() - self.period + timedelta(seconds=5)
        while self.ledger is None:
            message = self.messenger.get_message(self.id)
            if message is not None:
                self.process_message(message)
            if datetime.now() - t0 >= self.period:
                t0 = datetime.now()
                self.initiate_new_ballot()

    def process_message(self, message: Message):
        print(f"Agent #{self.id} received a {message.type} message from agent #{message.author_id}")
        if message.type is MessageType.LastVote:
            self.on_lastvote(message)
        elif message.type is MessageType.Voted:
            self.on_voted(message)
        elif message.type is MessageType.Success:
            self.on_success(message)

    def on_lastvote(self, message: Message):
        self.responses.append(message.last_vote)
        # If all responses are received
        if len(self.responses) == len(self.last_tried.quorum):
            # Sets the decree to satisfy B3
            responses = [r for r in self.responses if r is not None]
            if responses:
                self.last_tried.decree = max(*responses).ballot.decree
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
        # If the its ballot becomes successful, it sends a Success message to the whole assembly
        if message.vote.ballot == self.last_tried and self.last_tried.successful:
            response = Message(author_id=self.id, type=MessageType.Success, decree=self.last_tried.decree)
            for agent in self.assembly.agents:
                self.messenger.send_message(agent.id, response)

    def initiate_new_ballot(self):
        if self.last_tried is None:
            b = BallotNumber(0, self.id)
        else:
            b = BallotNumber(self.last_tried.number.ballot_id + 1, self.id)
        quorum = self.assembly.acceptors  # TODO: How to construct the quorum?
        self.last_tried = Ballot(b, Proposal(None), quorum, set())

        message = Message(
            author_id=self.id,
            type=MessageType.NextBallot,
            ballot_number=self.last_tried.number,
        )
        for acceptor in self.last_tried.quorum:
            self.messenger.send_message(acceptor.id, message)

    def make_proposal(self):
        return Proposal(self.id)


class Acceptor(Agent):
    def __init__(self, messenger: Messenger):
        super().__init__(messenger)
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
        # Upon receipt of a BeginBallot message,
        # Priest q casts his vote un the ballot
        message.ballot.voters.add(self)
        # Sets last_vote[q] to this vote
        self.last_vote = Vote(message.ballot, self)
        # And sends a Voted message to the proposer
        response = Message(author_id=self.id, type=MessageType.Voted, vote=self.last_vote)
        self.messenger.send_message(message.author_id, response)


class Assembly:
    def __init__(self, n_proposers: int, n_acceptors: int):
        self.messenger = ReliableMessenger()
        self.proposers = {Proposer(self.messenger, self) for _ in range(n_proposers)}
        self.acceptors = {Acceptor(self.messenger) for _ in range(n_acceptors)}
        self.learners = {}  # TODO: Implement learners
        # TODO: Implement failure of agents
        self.threads = dict()

    @property
    def agents(self):
        return self.proposers.union(self.acceptors).union(self.learners)

    def start(self):
        for agent in self.agents:
            self.threads[agent.id] = Thread(target=agent.start)
            self.threads[agent.id].start()

        for agent_id, thread in self.threads.items():
            thread.join()

        ledgers = set(agent.ledger.value for agent in self.agents)
        assert len(ledgers) == 1,\
            f"Failure : more than one proposal was accepted by a majority of voters ({ledgers})"
        return ledgers.pop()


if __name__ == '__main__':
    assembly1 = Assembly(n_proposers=2, n_acceptors=5)
    result = assembly1.start()
    print(result)

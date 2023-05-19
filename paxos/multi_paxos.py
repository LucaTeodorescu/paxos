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
        self.last_tried: List[Optional[Ballot]] = [None for _ in self.assembly.nb_instances]
        self.responses: List[List[Optional[Vote]]] = [list() for _ in self.assembly.nb_instances]

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
        if message.ballot_number in [ballot.number for ballot in self.last_tried]:
            idx = [ballot.number for ballot in self.last_tried].index(message.ballot_number)
            self.responses[idx].append(message.last_vote)
            # If all responses are received
            if len(self.responses[idx]) == len(self.last_tried[idx].quorum):
                # Sets the decree to satisfy B3
                ballots = [v.ballot for v in self.responses[idx] if v is not None]
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
                    ballot=self.last_tried[idx],
                    decree=self.last_tried.decree,
                )
                for acceptor in self.last_tried[idx].quorum:
                    self.messenger.send_message(acceptor.id, reponse)

    def on_voted(self, message: Message):
        # When the proposer receives a vote regarding its current ballot
        if message.vote.ballot.number in [ballot.number for ballot in self.last_tried]:
            idx = [ballot.number for ballot in self.last_tried].index(message.ballot_number)
            # It adds one voter
            self.last_tried[idx].voters.add(message.vote.acceptor)
            # If the ballot becomes successful
            if self.last_tried[idx].successful:
                # It sends a message to the whole assembly
                response = Message(author_id=self.id, type=MessageType.Success, decree=self.last_tried[idx].decree)
                for agent in self.assembly.agents:
                    self.messenger.send_message(agent.id, response)

    def initiate_new_ballot(self):
        quorum = self.create_random_quorum()
        print(f'{self} selected the following quorum : {quorum}')

        for idx in range(self.assembly.nb_instances):
            if self.last_tried[idx] is None:
                b = BallotNumber(0, self.id)
            else:
                b = BallotNumber(self.last_tried[idx].number.ballot_id + 1, self.id)

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


class Acceptor(Agent):
    # TODO : implement the agents in a similar way: most of the attributes will be replace by lists
    #  Acceptors can differentiate between the different instances:
    #  if a ballot has a ballot number (ballot_id, agent_id) then
    #  it is related to instance number ballot_id % nb_instances
    pass


class Assembly:
    # TODO: almost the same, with an additional attribute nb_instances corresponding to
    #  the number of parallel instances of the basic protocol
    pass

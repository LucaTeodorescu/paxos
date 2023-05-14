from __future__ import annotations

import asyncio
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Set, Optional
from dataclasses import dataclass
from collections import defaultdict
from numpy.random import random, exponential


# Dataclasses
@dataclass
class Proposal:
    value: Any


@dataclass(order=True)
class BallotNumber:
    ballot_id: int
    agent_id: int


@dataclass(order=True)
class Ballot:
    number: BallotNumber
    decree: Proposal
    quorum: Set[Agent]
    voters: Set[Agent]

    @property
    def successful(self) -> bool:
        return self.quorum.issubset(self.voters)


@dataclass(order=True)
class Vote:
    ballot: Ballot
    acceptor: Agent


# Messages
class MessageType(Enum):
    NextBallot = auto()
    LastVote = auto()
    BeginBallot = auto()
    Voted = auto()
    Success = auto()


@dataclass()
class Message:
    author_id: int
    type: MessageType
    # For NextBallot + LastVote
    ballot_number: Optional[BallotNumber] = None
    # For LastVote
    last_vote: Optional[Vote] = None
    # For BeginBallot
    ballot: Optional[Ballot] = None
    # For BeginBallot + Success
    decree: Optional[Proposal] = None
    # For Voted
    vote: Optional[Vote] = None


class Messenger(ABC):
    @abstractmethod
    def send_message(self, id_: int, message: Message):
        pass

    @abstractmethod
    def get_message(self, id_: int):
        pass


class ReliableMessenger(Messenger):
    def __init__(self):
        self.delivered = defaultdict(list)

    def send_message(self, id_: int, message: Message) -> None:
        self.delivered[id_].append(message)

    def get_message(self, id_: int) -> Optional[Message]:
        if self.delivered[id_]:
            return self.delivered[id_].pop(0)
        else:
            return None


class UnreliableMessenger(Messenger):
    def __init__(self, failure_prob: float = 0, avg_delay: float = 1):
        self.delivered = defaultdict(list)
        self.to_deliver = defaultdict(list)
        self.failure_prob = failure_prob
        self.avg_delay = avg_delay

    def send_message(self, id_: int, message: Message) -> None:
        if random() > self.failure_prob:
            self.to_deliver[id_].append(message)

    def get_message(self, id_) -> Optional[Message]:
        if self.delivered[id_]:
            return self.delivered[id_].pop(0)
        else:
            return None

    async def start(self):
        while True:
            to_do = []
            for id_, messages in self.to_deliver.items():
                for message in messages:
                    delay = self.avg_delay * exponential()

                    async def coro():
                        await asyncio.sleep(delay)
                        self.delivered[id_].append(message)

                    to_do.append(coro())

            await asyncio.gather(*to_do)


class Agent(ABC):
    counter = 0

    def __init__(self, messenger: Messenger):
        self.messenger = messenger
        self.ledger = None
        self.id = Agent.counter
        Agent.counter += 1

    def start(self) -> None:
        print(f"Agent #{self.id} started ({self.__class__.__name__})")
        while self.ledger is None:
            message = self.messenger.get_message(self.id)
            if message is not None:
                self.process_message(message)

    @abstractmethod
    def process_message(self, message: Message) -> None:
        pass

    def on_success(self, message) -> None:
        print(f"Agent #{self.id} was notified that decree {message.decree} was accepted.")
        self.ledger = message.decree

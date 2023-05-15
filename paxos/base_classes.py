from __future__ import annotations

import asyncio
from threading import Event
from datetime import datetime
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

    async def _start(self, event: Event):
        while not event.is_set():
            if sum(self.to_deliver.values(), []):
                print("[Messenger] Entrée boucle while")
                to_do = []
                for id_ in self.to_deliver.copy():
                    for message in self.to_deliver[id_]:
                        print(f"[Messenger] [{datetime.now()}] Message à délivrer :", message)

                        async def deliver_message():
                            print(f"[Messenger] [{datetime.now()}] Transfert du message", message, 'en cours')
                            delay = self.avg_delay * exponential()
                            await asyncio.sleep(delay)
                            self.delivered[id_].append(message)
                            self.to_deliver[id_].remove(message)
                            print(f"[Messenger] [{datetime.now()}] Transfert du message", message, 'terminé')

                        to_do.append(deliver_message())
                print(f"[Messenger] [{datetime.now()}]", len(to_do), "messages à délivrer")
                await asyncio.gather(*to_do)

    def start(self, event: Event):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._start(event))
        loop.close()


class Agent(ABC):
    counter = 0

    def __init__(self, messenger: Messenger):
        self.messenger = messenger
        self.ledger = None
        self.id = Agent.counter
        Agent.counter += 1

    def start(self, event: Event) -> None:
        print(f"Agent #{self.id} started ({self.__class__.__name__})")
        while not event.is_set():
            message = self.messenger.get_message(self.id)
            if message is not None:
                self.process_message(message)

    @abstractmethod
    def process_message(self, message: Message) -> None:
        pass

    def on_success(self, message) -> None:
        print(f"Agent #{self.id} was notified that decree {message.decree} was accepted.")
        self.ledger = message.decree

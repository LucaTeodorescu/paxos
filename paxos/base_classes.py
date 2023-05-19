from __future__ import annotations

import asyncio
from threading import Event
from time import sleep
from abc import ABC, abstractmethod
from enum import Enum, auto
from typing import Any, Set, Optional
from dataclasses import dataclass
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


@dataclass
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
    def register(self, agent_id: int) -> None:
        """Registers a new agent so that any agent can send message to them."""
        pass

    @abstractmethod
    def send_message(self, id_: int, message: Message) -> None:
        """Sends a message to an agent."""
        pass

    @abstractmethod
    def get_message(self, id_: int) -> Optional[Message]:
        """Allows an agent to get a message that was sent to them."""
        pass


class ReliableMessenger(Messenger):
    def __init__(self):
        self.delivered = dict()

    def register(self, agent_id: int) -> None:
        """Registers a new agent so that any agent can send message to them."""
        self.delivered[agent_id] = []

    def send_message(self, dest_id: int, message: Message) -> None:
        """Sends a message to an agent."""
        if dest_id in self.delivered:
            self.delivered[dest_id].append(message)

    def get_message(self, dest_id: int) -> Optional[Message]:
        """Allows an agent to get a message that was sent to them."""
        if self.delivered[dest_id]:
            return self.delivered[dest_id].pop(0)
        else:
            return None


class UnreliableMessenger(Messenger):
    def __init__(self, failure_rate: float = 0, max_delay: float = 10):
        self.failure_rate = failure_rate
        self.max_delay = max_delay

        self.delivered = dict()
        self.to_deliver = dict()

    def register(self, agent_id: int):
        """Registers a new agent so that any other agent can send message to them."""
        self.to_deliver[agent_id] = []
        self.delivered[agent_id] = []

    def send_message(self, dest_id: int, message: Message) -> None:
        """Sends a message to an agent."""
        if dest_id in self.to_deliver and random() > self.failure_rate:
            self.to_deliver[dest_id].append(message)
        else:
            print(f"Agent #{message.author_id} failed to message agent #{dest_id}")

    def get_message(self, dest_id) -> Optional[Message]:
        """Allows an agent to get a message that was sent to them."""
        if self.delivered[dest_id]:
            return self.delivered[dest_id].pop(0)
        else:
            return None

    async def deliver_message(self, dest_id: int, message: Message):
        """Delivers a message: transfers it from `to_deliver' to `delivered'."""
        # print(f"[Messenger] [{datetime.now()}] Debut du transfert :", message)
        delay = min(self.max_delay, self.max_delay * exponential() / 2)
        await asyncio.sleep(delay)
        self.delivered[dest_id].append(message)
        try:
            self.to_deliver[dest_id].remove(message)
        except ValueError:
            # This error can be raised if the addressee restarts, which empties its message lists
            pass
        # print(f"[Messenger] [{datetime.now()}] Fin du transfert :", message)

    async def _start(self, event: Event):
        while not event.is_set():
            if sum(self.to_deliver.values(), []):
                to_do = []
                for dest_id in self.to_deliver.copy():
                    for message in self.to_deliver[dest_id]:
                        to_do.append(self.deliver_message(dest_id, message))
                await asyncio.gather(*to_do)

    def start(self, event: Event):
        loop = asyncio.new_event_loop()
        loop.run_until_complete(self._start(event))
        loop.close()


class Agent(ABC):
    counter = 0

    def __init__(self, messenger: Messenger, failure_rate: float = 0, avg_failure_duration: float = 5):
        self.ledger = None
        self.messenger = messenger
        self.failure_rate = failure_rate
        self.avg_failure_duration = avg_failure_duration

        self.id = Agent.counter
        Agent.counter += 1

    def __repr__(self):
        return f'Agent #{self.id}'

    def start(self, event: Event) -> None:
        print(f"Agent #{self.id} started ({self.__class__.__name__})")
        self.messenger.register(self.id)
        while not event.is_set():
            # Process messages received
            message = self.messenger.get_message(self.id)
            if message is not None:
                self.process_message(message)
            # And maybe fail
            if random() < self.failure_rate:
                print(f"Agent #{self.id} failed ({self.__class__.__name__})")
                sleep(self.avg_failure_duration * exponential())
                break

        # If the agent failed, it starts again
        if not event.is_set():
            self.start(event)

    @abstractmethod
    def process_message(self, message: Message) -> None:
        pass

    def on_success(self, message) -> None:
        print(f"Agent #{self.id} was notified that decree {message.decree} was accepted.")
        self.ledger = message.decree

from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any, Dict, Optional, List
from datetime import datetime
from benchmark.stats import Stats


@dataclass
class Message:
    """Custom message structure for queue operations."""
    id: str
    content: Any
    produce_time: datetime
    consume_time: datetime
    data: bytes = b'\x00' * (10 * 1024 * 1024)  # Default value of 10MB
    
    def to_dict(self) -> Dict[str, Any]:
        """Convert message to dictionary for serialization."""
        return {
            "id": self.id,
            "content": self.content,
            "produce_time": self.produce_time,
            "consume_time": self.consume_time
        }
    
    @classmethod
    def from_dict(cls, data: Dict[str, Any]) -> 'Message':
        """Create a Message instance from dictionary."""
        return cls(
            id=data.get("id", ""),
            content=data.get("content"),
            produce_time=data.get("produce_time"),
            consume_time=data.get("consume_time")
        )


class MessageQueueBase(ABC):
    """
    Base class for message queue implementations
    """
    def __init__(self, name: str):
        self.connected = False
        self.stats = None
        self.consumer_count = -1
        self.name = name
        self.running_consumers = 0
    
    @abstractmethod
    def connect(self) -> bool:
        """
        Connect to the message queue service.
        
        Args:
            config: Configuration parameters for connecting to the queue
            
        Returns:
            bool: True if connection was successful, False otherwise
        """
        pass

    @abstractmethod
    def produce(self, messages: List[Message]) -> bool:
        """
        Produce messages to the queue.
        
        Args:
            messages: List of Message objects to be sent
            
        Returns:
            bool: True if messages were sent successfully, False otherwise
        """
        pass

    @abstractmethod
    def consume(self):
        """
        Consume messages from the queue.
        
        Returns:
            List[Message]: A list of consumed Message objects
        """
        pass

    def close(self) -> bool:
        """
        Close the connection to the message queue.
        
        Returns:
            bool: True if connection was closed successfully, False otherwise
        """
        if self.connected:
            self.connected = False
            return True
        return False

    def set_stats(self, stats: Stats) -> None:
        """
        Set statistics for the message queue.
        
        Args:
            stats: Stats object containing statistics
        """
        self.stats = stats
    
    def get_stats(self) -> Stats:
        """
        Get the current statistics.
        
        Returns:
            Stats: Stats object containing statistics
        """
        return self.stats
    
    def is_connected(self) -> bool:
        """
        Check if queue is connected.
        
        Returns:
            bool: True if connected, False otherwise
        """
        return self.connected

    def stop_consumer(self, index: int) -> None:
        """
        Stop a consumer.
        """
        
        pass

    def get_consumed_messages(self) -> List[Message]:
        """
        Get the consumed messages.
        """
        pass

    def get_num_consumed_messages(self) -> int:
        pass

    def is_consuming(self) -> bool:
        """
        Check if the queue is consuming messages.
        """
        return self.running_consumers > 0
        
#!/usr/bin/env python3
"""
0. Create a Cache class. In the __init__ method, store an instance of
the Redis client as a private variable named _redis (using redis.Redis())
and flush the instance using flushdb.

Create a store method that takes a data argument and returns a string.
The method should generate a random key (e.g. using uuid), store the
input data in Redis using the random key and return the key.

Type-annotate store correctly. Remember that data can be a str,
bytes, int or float.


1. Redis only allows to store string, bytes and numbers (and lists thereof).
Whatever you store as single elements, it will be returned as a byte string.
Hence if you store "a" as a UTF-8 string, it will be returned as b"a" when
retrieved from the server.

In this exercise we will create a get method that take a key string argument
and an optional Callable argument named fn. This callable will be used to
convert the data back to the desired format.

Remember to conserve the original Redis.get behavior if the key does not exist.
Also, implement 2 new methods: get_str and get_int that will automatically
parametrize Cache.get with the correct conversion function.
"""
import redis
from uuid import uuid4
from typing import Union, Callable, Optional
from functools import wraps


def count_calls(method: Callable) -> Callable:
    """Decorator that counts the number of times a method is called."""
    key = method.__qualname__

    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """Wrapper function that increments the call count in Redis."""
        self._redis.incr(key)
        return method(self, *args, **kwargs)

    return wrapper


def call_history(method: Callable) -> Callable:
    """Decorator that stores the history of inputs and outputs for a method."""
    @wraps(method)
    def wrapper(self, *args, **kwargs):
        """Wrapper function that logs inputs and outputs in Redis."""
        input_data = str(args)
        self._redis.rpush(method.__qualname__ + ":inputs", input_data)
        output_data = str(method(self, *args, **kwargs))
        self._redis.rpush(method.__qualname__ + ":outputs", output_data)
        return output_data

    return wrapper


def replay(fn: Callable):
    """Displays the history of calls for a particular function."""
    r = redis.Redis()
    function_name = fn.__qualname__
    value = r.get(function_name)
    try:
        value = int(value.decode("utf-8"))
    except Exception:
        value = 0

    print("{} was called {} times:".format(function_name, value))
    inputs = r.lrange("{}:inputs".format(function_name), 0, -1)
    outputs = r.lrange("{}:outputs".format(function_name), 0, -1)

    for input_data, output_data in zip(inputs, outputs):
        try:
            input_data = input_data.decode("utf-8")
        except Exception:
            input_data = ""

        try:
            output_data = output_data.decode("utf-8")
        except Exception:
            output_data = ""

        print("{}(*{}) -> {}".format(function_name, input_data, output_data))


class Cache:
    """A class that provides a simple caching mechanism using Redis."""

    def __init__(self):
        """Initializes the Cache instance and flushes the Redis database."""
        self._redis = redis.Redis()
        self._redis.flushdb()

    @count_calls
    @call_history
    def store(self, data: Union[str, bytes, int, float]) -> str:
        """Stores data in Redis with a randomly generated key.

        Args:
            data (Union[str, bytes, int, float]): The data to be stored.

        Returns:
            str: The generated key associated with the stored data.
        """
        random_key = str(uuid4())
        self._redis.set(random_key, data)
        return random_key

    def get(self, key: str,
            fn: Optional[Callable] = None
            ) -> Union[str, bytes, int, float]:
        """Retrieves data from Redis and converts it using an
        optional function.

        Args:
            key (str): The key associated with the data to retrieve.
            fn (Optional[Callable]): An optional function to convert the data.

        Returns:
            Union[str, bytes, int, float]: The retrieved data,
            converted if a function is provided.
        """
        value = self._redis.get(key)
        if fn:
            value = fn(value)
        return value

    def get_str(self, key: str) -> str:
        """Retrieves data from Redis and converts it to a string.

        Args:
            key (str): The key associated with the data to retrieve.

        Returns:
            str: The retrieved data as a UTF-8 string.
        """
        value = self._redis.get(key)
        return value.decode("utf-8")

    def get_int(self, key: str) -> int:
        """Retrieves data from Redis and converts it to an integer.

        Args:
            key (str): The key associated with the data to retrieve.

        Returns:
            int: The retrieved data as an integer, or 0 if conversion fails.
        """
        value = self._redis.get(key)
        try:
            value = int(value.decode("utf-8"))
        except Exception:
            value = 0
        return value

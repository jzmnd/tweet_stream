#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
twitter_app.py
Streams twitter data from the API using tweepy and sends data to socket.

Attributes:
    HOST (str): Host name for stream service
    PORT (int): Port for stream service
"""
import sys
import socket
import logging

import yaml
from tweepy import OAuthHandler
from tweepy import Stream
from tweepy.streaming import StreamListener


HOST = 'localhost'
PORT = 5055             # Reserve a port for your service
logging.basicConfig(level=logging.INFO)


def load_yaml(filename):
    """Load yaml config file with the authentication credentials and search terms

    Args:
        filename (str): Config file pathname

    Returns:
        dict: Config dictionary
    """
    with open(filename, 'r') as file:
        data = yaml.load(file)
    return data['user'], data['secrets'], data['search']


class TweetsListener(StreamListener):
    """Tweepy listener class that inherits from the StreamListener

    Attributes:
        client_socket (TYPE): Description
    """

    def __init__(self, client_socket):
        super().__init__()
        self.client_socket = client_socket

    def on_data(self, raw_data):
        """Called when raw data is received from the connection

        Args:
            raw_data (bytes): Raw data stream from Twitter API
        """
        try:
            self.client_socket.send(raw_data.encode('utf-8'))
        except BaseException as exception:
            logging.error("Error on_data: %s", exception)

    def on_error(self, status_code):
        logging.error("Server error: %i", status_code)
        if status_code == 420:
            # returning False in on_data disconnects the stream in case of API limit
            return False
        return True

    def on_exception(self, exception):
        logging.error("Exception: %s", exception)


def send_data(client_socket, search, secrets):
    """Creates the stream with authentication and filters the data based on `track`

    Args:
        client_socket (TYPE): Client socket
        search (dict): Search info dicitonary
        secrets (dict): API credentials dicitonary
    """
    auth = OAuthHandler(secrets['consumer_key'],
                        secrets['consumer_secret'])
    auth.set_access_token(secrets['access_token'],
                          secrets['access_secret'])

    twitter_stream = Stream(auth=auth, listener=TweetsListener(client_socket))
    twitter_stream.filter(track=search['track'], languages=search['languages'])


def run_listener(search, secrets):
    """Run the tweet listener

    Args:
        search (dict): Search info dicitonary
        secrets (dict): API credentials dicitonary
    """
    sock = socket.socket()     # Create a socket object
    sock.bind((HOST, PORT))    # Bind to the port

    logging.info("Listening on port: %i", PORT)

    sock.listen(1)                    # Now wait for client connection
    conn, addr = sock.accept()        # Establish connection with client

    logging.info("Received request from: %s", str(addr))

    send_data(conn, search, secrets)


if __name__ == "__main__":
    USER, SECRETS, SEARCH = load_yaml(sys.argv[1])
    run_listener(SEARCH, SECRETS)

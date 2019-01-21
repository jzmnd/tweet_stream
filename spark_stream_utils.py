#!/usr/bin/env python
'''
spark_stream_utils.py

Utility functions for extracting data from twitter json
'''

def get_text(data):
    # Try for extended text of original tweet, if RT'd (streamer)
    try: return data['retweeted_status']['extended_tweet']['full_text']
    except KeyError: pass
    # Try for extended text of an original tweet, if RT'd (REST API)
    try: return data['retweeted_status']['full_text']
    except KeyError: pass
    # Try for extended text of an original tweet (streamer)
    try: return data['extended_tweet']['full_text']
    except KeyError: pass
    # Try for extended text of an original tweet (REST API)
    try: return data['full_text']
    except KeyError: pass
    # Try for basic text of original tweet if RT'd 
    try: return data['retweeted_status']['text']
    except KeyError: pass
    # Try for basic text of an original tweet
    try: return data['text']
    except KeyError: return ''


def get_location(data):

    # Try for geotagged exact latitude/longitude
    try:
        a = data['retweeted_status']['extended_tweet']['geo']
        if a: return ('geo', a['coordinates'])
    except KeyError: pass
    try:
        a = data['retweeted_status']['geo']
        if a: return ('geo', a['coordinates'])
    except KeyError: pass
    try:
        a = data['extended_tweet']['geo']
        if a: return ('geo', a['coordinates'])
    except KeyError: pass
    try:
        a = data['geo']
        if a: return ('geo', a['coordinates'])
    except KeyError: pass
    # Try for place information in the tweet
    try:
        a = data['retweeted_status']['extended_tweet']['place']
        if a: return ('place', a['country'])
    except KeyError: pass
    try:
        a = data['retweeted_status']['place']
        if a: return ('place', a['country'])
    except KeyError: pass
    try:
        a = data['extended_tweet']['place']
        if a: return ('place', a['country'])
    except KeyError: pass
    try:
        a = data['place']
        if a: return ('place', a['country'])
    except KeyError: pass
    # Try for user location information
    try:
        a = data['retweeted_status']['extended_tweet']['user']['location']
        if a: return ('user', a)
    except KeyError: pass
    try:
        a = data['retweeted_status']['user']['location']
        if a: return ('user', a)
    except KeyError: pass
    try:
        a = data['extended_tweet']['user']['location']
        if a: return ('user', a)
    except KeyError: pass
    try:
        a = data['user']['location']
        if a: return ('user', a)
    except KeyError: pass

    return ('final', None)

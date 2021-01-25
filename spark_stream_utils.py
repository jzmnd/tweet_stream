#!/usr/bin/env python
"""
spark_stream_utils.py

Utility functions for extracting data from twitter json
"""


def get(data, keys):
    """Recursively search through nested dictionary based on a list of keys.
    Returns None if key is not found."""
    try:
        data0 = data[keys[0]]
    except KeyError:
        return
    if len(keys) == 1:
        return data0
    return get(data0, keys[1:])


TEXT_KEYS_CASCADE = [
    # Try for extended text of original tweet, if RT'd (streamer)
    ["retweeted_status", "extended_tweet", "full_text"],
    # Try for extended text of an original tweet, if RT'd (REST API)
    ["retweeted_status", "full_text"],
    # Try for extended text of an original tweet (streamer)
    ["extended_tweet", "full_text"],
    # Try for extended text of an original tweet (REST API)
    ["full_text"],
    # Try for basic text of original tweet if RT'd
    ["retweeted_status", "text"],
    # Try for basic text of an original tweet
    ["text"],
]


def get_text(data):
    """Get tweet text"""

    for keys in TEXT_KEYS_CASCADE:
        text = get(data, keys)
        if text:
            return text
    return ""


LOCATION_KEYS_CASCADE_GEO = [
    # Try for geotagged exact latitude/longitude
    ["retweeted_status", "extended_tweet", "geo"],
    ["retweeted_status", "geo"],
    ["extended_tweet", "geo"],
    ["geo"],
]
LOCATION_KEYS_CASCADE_PLACE = [
    # Try for place information in the tweet
    ["retweeted_status", "extended_tweet", "place"],
    ["retweeted_status", "place"],
    ["extended_tweet", "place"],
    ["place"],
]
LOCATION_KEYS_CASCADE_USER = [
    # Try for user location information
    ["retweeted_status", "extended_tweet", "user", "location"],
    ["retweeted_status", "user", "location"],
    ["extended_tweet", "user", "location"],
    ["user", "location"],
]


def get_location(data):
    """Get tweet location"""

    for keys in LOCATION_KEYS_CASCADE_GEO:
        loc = get(data, keys)
        if loc:
            return ("geo", loc["coordinates"])
    for keys in LOCATION_KEYS_CASCADE_PLACE:
        loc = get(data, keys)
        if loc:
            return ("place", loc["country"])
    for keys in LOCATION_KEYS_CASCADE_USER:
        loc = get(data, keys)
        if loc:
            return ("user", loc)
    return ("final", None)

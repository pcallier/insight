#!/usr/bin/env python
"""twitter_api_func.py

Has Twitter API functionality in it, what luck. get_api() returns a 
tweepy api object
"""

import tweepy
import codecs


def get_api(app_key=None, app_secret=None, access_key=None, access_secret=None, *args, **kwargs):
    """Return a tweepy API object using supplied keys and secrets or 
    looking for them in pwd if None"""
    try:
        if app_key == None:
            with open("app.key", "r") as f:
                app_key = ('\n'.join(f.readlines())).strip()
        if app_secret == None:
            with open("app.secret", "r") as f:
                app_secret = ('\n'.join(f.readlines())).strip()
    except IOError:
        raise IOError("Key or secret not found")

    auth = tweepy.OAuthHandler(app_key, app_secret)

    try:
        if access_key == None:
            with open("access.key", "r") as f:
                access_key = ('\n'.join(f.readlines())).strip()
        if access_secret == None:
            with open("access.secret", "r") as f:
                access_secret = ('\n'.join(f.readlines())).strip()
    except IOError:
        print >> sys.stderr, "No access token or secret, will try to authenticate user."

    if access_key == None or access_secret == None:            
        try:
            redirect_url = auth.get_authorization_url()
        except tweepy.TweepError:
            print 'Error! Failed to get request token.'
            
        print "Go to {} and input the PIN code you find there.".format(redirect_url)

        verifier = raw_input('Verifier:')

        try:
            access_token = auth.get_access_token(verifier)
            with open("access.key", "w") as f:
                print >> f, access_token[0]
            with open("access.secret", "w") as f:
                print >> f, access_token[1]
        except tweepy.TweepError:
            print "Error! Failed to get access token."
    else:
        auth.set_access_token(access_key,access_secret)
        
    api = tweepy.API(auth, *args, **kwargs)
    return api           

#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import os
import tweepy
import codecs
import re
import time
import itertools
import getpass
import multiprocessing
from multiprocessing.managers import SyncManager
import signal
import contextlib
from twitter_api_func import get_api
import logging
logging.basicConfig(level=logging.WARNING)
import psycopg2 as mdb

bounding_boxes = { 'usa': (-125.6791025,25.4180700649,-66.885417,
                           49.3284551525) }
all_sites = list(itertools.chain.from_iterable(
    k for k in bounding_boxes.itervalues()))

def connect_db():
    con = mdb.connect("dbname=tweets user=patrick")
    assert con is not None
    return con

def mgr_init():
    signal.signal(signal.SIGINT, signal.SIG_IGN)

def normalize_text(text):
    blank_these = re.compile(r"(')+")
    space_these = re.compile(r"(\s+)")
    return space_these.sub(" ", blank_these.sub("", text))

def translate_date(date):
    """ NOT USED
    Translate incoming date format to MySQL-compatible format:
    Twitter: Wed Aug 27 13:08:45 +0000 2008
    MySQL: 2012-12-31 11:30:45"""
    
    year = re.sub(r".*([0-9]{4})$", r"\1", date)
    mon = re.sub(r".*([0-9]{4})$", r"\1", date)
    day = re.sub(r".*([0-9]{4})$", r"\1", date)
    return ""

def ingest_tweet(status, con):
    # record tweet in tweets db (represented by con)
    
    with con.cursor() as cur:
        if getattr(status, "retweeted_status", None) is not None or getattr(status, 'coordinates', None) is None:
            return
        logging.debug(status.id_str)
        in_reply_to_tweet_id = getattr(status,'in_reply_to_status_id_str','')
        query = (u"INSERT INTO tweets (tweet_id, text, created_at,"
                    u"user_id, language, lat, long, in_reply_to_tweet_id) "
                    u" VALUES ('{}','{}','{}','{}','{}','{}','{}','{}')"
                    ).format(
                    status.id_str,
                    normalize_text(status.text),
                    status.created_at,
                    status.user.id_str,
                    status.lang,
                    status.coordinates['coordinates'][0],
                    status.coordinates['coordinates'][1],
                    in_reply_to_tweet_id)
        logging.debug(query)
        cur.execute(query)
    con.commit()
    #try:
        #with con.cursor() as cur:
            #cur.execute(("SELECT tweet_id FROM tweets WHERE"
                              #"tweet_id={}").format(status.in_reply_to_id_str))
                                
            #if status.in_reply_to_id_str != "" and in_reply_to_recorded is not None
            
    ## retrieve in_reply_to tweets

 
def do_ingestion(q):
    with connect_db() as con:
        while True:
            logging.debug("Listening...")
            try:
                status = q.get()
            except EOFError as e:
                logging.error("Unexpected EOF, restarting...")
                return
            logging.debug(status.id_str)
            ingest_tweet(status, con)
            q.task_done()

#override tweepy.StreamListener to add logic to on_status
class MyStreamListener(tweepy.StreamListener):
    def __init__(self, *args, **kwargs):
        # init ingestor process, create tweet queue
        manager = SyncManager()
        manager.start(mgr_init)
        self.tweet_queue = manager.Queue()
        self.ingestion_process = multiprocessing.Process(target=do_ingestion, args=(self.tweet_queue,))
        self.ingestion_process.start()
        
        # call superclass init
        tweepy.StreamListener.__init__(self, *args, **kwargs)

    def is_running(self):
        return self.ingestion_process.is_alive()

    def on_status(self, status):
        if status.coordinates != None and status.coordinates['type'] == 'Point' and hasattr(status, 'lang') and status.lang=='en':
           self.tweet_queue.put(status)

            
    def on_error(self, status_code):
        if status_code == 420:
            #returning False in on_data disconnects the stream
            return False
            
    def on_warning(self, warning):
        logging.warning("Warning: {}".format(warning))
        
    def on_limit(self, track):
        logging.warning("Limit: {}".format(track))
    
    def on_disconnect(self, notice):
        logging.error("Disconnect notice: {}".format(str(notice)))
        return False

def main():
    api = get_api()
    us_listener = MyStreamListener()
    us_stream = tweepy.Stream(auth=api.auth, listener=us_listener)
    try:
        us_stream.filter(locations=all_sites, async=True)
        while us_stream.running and us_listener.is_running():
            time.sleep(0.1)
    finally:
        us_stream.disconnect()
        if us_listener.is_running():
            us_listener.tweet_queue.join()
        logging.debug("Bye!")

if __name__ == "__main__":
    while True:
        main()
        print >> sys.stderr, "Waiting, then restarting..."
        time.sleep(15)

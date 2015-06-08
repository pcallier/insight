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
import logging
logging.basicConfig(level=logging.DEBUG)
import psycopg2 as mdb
from twitter_api_func import get_api
from get_users import add_user_to_db
from shared_utilities import normalize_text, connect_db

bounding_boxes = { 'usa': (-125.6791025,25.4180700649,-66.885417,
                           49.3284551525) }
user_table = "users"

all_sites = list(itertools.chain.from_iterable(
    k for k in bounding_boxes.itervalues()))


def mgr_init():
    signal.signal(signal.SIGINT, signal.SIG_IGN)



def translate_date(date):
    """ NOT USED
    Translate incoming date format to MySQL-compatible format:
    Twitter: Wed Aug 27 13:08:45 +0000 2008
    MySQL: 2012-12-31 11:30:45"""
    
    year = re.sub(r".*([0-9]{4})$", r"\1", date)
    mon = re.sub(r".*([0-9]{4})$", r"\1", date)
    day = re.sub(r".*([0-9]{4})$", r"\1", date)
    return ""

tweets = []

def record_tweets(statuses, cur, con):
    cur.execute("SELECT tweet_id FROM tweets")
    tweet_ids = set(zip(*cur.fetchall())[0])
    statuses = [st for st in statuses if st.id_str not in tweet_ids]
    
    # check for duplicates w/i stream
    new_ids = [st.id_str for st in statuses]
    status_dict = dict()
    for id_str in set(new_ids):
        status = [st for st in statuses if st.id_str == id_str][0]
        status_dict[id_str] = status
    
    
    query = (u"INSERT INTO tweets (tweet_id, text, created_at,"
            u"user_id, language, long, lat, in_reply_to_tweet_id) VALUES ") + \
            u", ".join([u"('{}','{}','{}','{}','{}','{}','{}','{}')".format(
            tw.id_str,
            normalize_text(tw.text),
            tw.created_at,
            tw.user.id_str,
            tw.lang,
            tw.coordinates['coordinates'][0],
            tw.coordinates['coordinates'][1],
            tw.in_reply_to_tweet_id) for tw in status_dict.itervalues()])
    logging.debug(query)
    cur.execute(query)
    con.commit()
    
    # add users to database
    for tw in status_dict.itervalues():
        try:
            logging.debug("Adding user {}".format(tw.user.id_str))
            add_user_to_db(tw.user, cur)
        except mdb.IntegrityError:
            logging.warning("User already in db")
            logging.debug("Detail: ", exc_info=True)
            con.rollback()
        con.commit()


def ingest_tweet(status, con):
    global tweets
    # record tweet in tweets db (represented by con)
    with con.cursor() as cur:
        if getattr(status, "retweeted_status", None) is not None:
            return
        setattr(status, 'in_reply_to_tweet_id', getattr(status,'in_reply_to_status_id_str',''))
        tweets.append(status)
        #if status.in_reply_to_tweet_id != '':
            #reply_tweets.append(status)

        if len(tweets) > 40:
            record_tweets(tweets, cur, con)
            tweets = []
    con.commit()



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
    #global reply_tweets
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

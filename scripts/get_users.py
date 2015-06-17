#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import os
import logging
logging.basicConfig(level=logging.DEBUG)
import re
import time
import datetime
import tweepy
import psycopg2 as mdb
from twitter_api_func import get_api
from shared_utilities import normalize_text, connect_db
logging.getLogger("oauthlib").setLevel(logging.WARNING)
logging.getLogger("requests_oauthlib").setLevel(logging.WARNING)

def get_missing_users(con):
    logging.debug("Getting missing users")
    with con.cursor() as cur:
        cur.execute("select distinct tw.user_id from tweets as "
            "tw where not exists (select us.user_id from users as us "
            "where tw.user_id = us.user_id)")
        logging.info(cur.rowcount)
        missing_user_ids = zip(*cur.fetchall())[0]
        return missing_user_ids

def add_user_to_db(user, cur, user_tbl="users"):
    """Takes an actual tweepy user object and adds certain inforation to
    the users table of the connected database using the supplied cursor)"""
    cur.execute((u"INSERT INTO {} (user_id, screen_name, name,"
                u"followers_count, friends_count, description,"
                u"timezone, place) VALUES"
                u" ('{}', '{}', '{}', '{}','{}','{}','{}', '{}')").format(
                user_tbl,
                user.id_str,
                user.screen_name,
                normalize_text(user.name),
                user.followers_count,
                user.friends_count,
                normalize_text(getattr(user,'description','')),
                normalize_text(getattr(user,'timezone','')),
                normalize_text(getattr(user,'location','None'))))
                
def update_or_add_users(ids, api, con, update_or_add_func=add_user_to_db):
    logging.debug("Updating users table")
    logging.debug(ids)
    with con.cursor() as cur:
        while len(ids) > 0:
            ids_to_look_up = ids[0:min(len(ids),100)]
            #logging.debug(len(ids_to_look_up))
            try:
                user_result = api.lookup_users(ids_to_look_up)
            except tweepy.TweepError as e:
                logging.warning("Twitter error", exc_info=True)
                
                # Rate limit
                if e[0][0]['code'] == 88:
                    logging.warning(api.rate_limit_status()['resources'][u'users'][u'/users/lookup'])
                    time.sleep(float(api.rate_limit_status()['resources']['users'][u'/users/lookup']['reset']) - time.time())
                    continue
                # Users not found
                if e[0][0]['code'] == 11:
                    try:
                        ids = ids[100:]
                    except:
                        ids = []
                continue
                
            for user in user_result:
                try:
                    logging.debug("Adding/updating user...")
                    update_or_add_func(user, cur)
                except mdb.IntegrityError:
                    logging.warning("IntegrityError")
                    logging.debug("",exc_info=True)
                    con.rollback()
                con.commit()
            try:
                ids = ids[100:]
            except:
                ids = []

def fill_in_field(api, con, user_tbl="users", key_col="user_id", field_to_fill="place"):
    with con.cursor() as cur:
        cur.execute("SELECT {0}, {1} FROM {2} WHERE {1}=''".format(
            key_col, field_to_fill, user_tbl))
        users_to_update = zip(*cur.fetchall())
    update_or_add_users(users_to_update[0], api, con, update_user_place)
 
def update_user_place(user, cur):
    """User must already exist in database, updates their place field 
    based on location in User object user"""
    place = normalize_text(getattr(user, 'location', u''))
    if place == '':
        place = 'None'
    query = u"UPDATE users SET place='{}' WHERE user_id='{}'".format(
        place, user.id_str)
    if place != "None":
        logging.debug(query)
    cur.execute(query)
                        

def get_last_tweets(con, api):
    """Connect to twitter and get date of last tweet for each user"""
    # get user ids
    with con.cursor() as cur:
        cur.execute("SELECT user_id FROM users WHERE last_tweet_at is NULL")
        user_ids = zip(*cur.fetchall())[0]
    
    while len(user_ids) > 0:
        cur_users = user_ids[:100]
        try:
            user_objs = api.lookup_users(cur_users)
            def getdate(user):
                if getattr(user, 'status', None):
                    return user.status.created_at
                else:
                    return '2000-01-01 12:00:00'
            last_tweet_at = { user.id_str : getdate(user) for user 
                                in user_objs }
        except tweepy.TweepError as e:
            # no matches at all
            print dir(e)
            print e.args
            print e.message
            try:
                if e.message[0]['code'] == 17:
                    last_tweet_at = { uid : '2001-01-01 12:00:00' 
                                        for uid in cur_users }
            except:
                try:
                    if e.message.find("HTTPSConnectionPool") != -1:
                        continue
                except:
                    print e
                raise
                
        
        with con.cursor() as cur:
            for user_id, tw_date in last_tweet_at.iteritems():
                cur.execute(("UPDATE users SET last_tweet_at = '{}' "
                "WHERE user_id = '{}'").format(tw_date, user_id))
        con.commit()
        user_ids = user_ids[100:]

if __name__ == "__main__":
    if sys.argv[1] == 'getmissing':
        with connect_db() as con:
            update_or_add_users(list(set(get_missing_users(con))), get_api(), con)
    elif sys.argv[1] == 'getlasttweet':
        with connect_db() as con:
            get_last_tweets(con, get_api(wait_on_rate_limit=True))
    
    #    fill_in_field(get_api(), con)

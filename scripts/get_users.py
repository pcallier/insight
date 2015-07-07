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
logging.getLogger("tweepy").setLevel(logging.WARNING)
logging.getLogger("requests").setLevel(logging.WARNING)


import numpy as np

tw_dt_format = '%Y-%m-%d %H:%M:%S'

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
                        

def user_ids_query(con, query):
    # get user ids
    with con.cursor() as cur:
        #cur.execute("SELECT user_id FROM users WHERE screen_name IS NULL OR profile_pic_url is NULL OR created_at IS NULL")
        #cur.execute("SELECT user_id FROM users")
        cur.execute(query)
        user_ids = zip(*cur.fetchall())[0]
    return user_ids

def add_pic_name_etc(con, user_objs):    
    pics = { user.id_str : getattr(user, 'profile_image_url', None) for user 
                            in user_objs }
    names = { user.id_str : getattr(user, 'screen_name', None) for user 
                        in user_objs }
    realnames = { user.id_str : getattr(user, 'name', None) for user in user_objs }
    created_at = { user.id_str : getattr(user, 'created_at', None) for user 
                        in user_objs }
    # add to db where necessary. just use keys from pics (UGLY programming)
    with con.cursor() as cur:
        for user_id in [ user.id_str for user in user_objs ]:
            try:
                if pics[user_id]:
                    #logging.warning(pics[user_id])
                    query = u"UPDATE users SET profile_pic_url='{}' WHERE user_id='{}'".format(
                    pics[user_id], user_id)
                #cur.execute(query)
                #con.commit()
            except mdb.IntegrityError:
                logging.warning("Blech", exc_info=True)
            try:
                if names[user_id]:
                    query = u"UPDATE users SET screen_name='{}' WHERE user_id='{}'".format(
                        names[user_id], user_id)
                    #cur.execute(query)
                    #con.commit()
            except mdb.IntegrityError:
                logging.warning("Blech is my sn", exc_info=True)
            try:
                if created_at[user_id]:
                    query = u"UPDATE users SET created_at='{}' WHERE user_id='{}'".format(
                        created_at[user_id], user_id)
                    #cur.execute(query)
                    #con.commit()
            except mdb.IntegrityError:
                logging.warning("Blech is my birthday", exc_info=True)
            try:
                if realnames[user_id]:
                    query = u"UPDATE users SET name=$blahh${}$blahh$ WHERE user_id='{}'".format(
                        realnames[user_id], user_id)
                    cur.execute(query)
                    #con.commit()
            except mdb.IntegrityError:
                logging.warning("Blech is my naame", exc_info=True)   
                
                
def get_user_name_created_at_and_pic(con, api):
    """Connect to twitter and get screen name and profile image for user,
    store in DB. """
    lookup_users_do_something(con, api, 
                              "SELECT user_id FROM users WHERE name ~ '[0-9]{4,}'",
                              add_pic_name_etc) 

def get_415_5_tweets(con, user_objs):
    """Get up to five most recent tweets posted before 4/15/15 for
    each user. Calculate freq of tweeting over this sample,
    record in db"""
    
    # look up user in database, get most recent tweet
    
    refdate = datetime.datetime(2015,04,15)
    twapi = get_api(wait_on_rate_limit=True)
    for user in user_objs:
        logging.debug(user.id_str)
        with con.cursor() as cur:
            # root through user timeline for tweets before reference date
            request_size = 200
            no_requested = 0
            no_to_get = 5
            max_id = None
            twts_to_return = []
            while no_requested < 3200:
                logging.debug(no_requested)
                try:
                    twts = twapi.user_timeline(user_id=user.id_str, count=request_size, max_id=max_id)
                    no_requested = no_requested + request_size
                    # find if any are < ref date
                    twts_less_than_date = [tw for tw in twts if tw.created_at < refdate]
                    max_id = twts[np.argmin([tw.created_at for tw in twts])].id_str
                    if len(twts_less_than_date) == 0:
                        continue
                    if len(twts_less_than_date) > 0:
                        # get as many as you can, return if enough
                        twts_to_return = twts_less_than_date + twts_to_return
                        if len(twts_to_return) >= no_to_get:
                            # TODO: store in database
                            twts_to_return = twts_to_return[-no_to_get:]
                            tmstamp = datetime.datetime.strftime(min([tw.created_at 
                                                        for tw in twts_to_return]),
                                                        '%Y-%m-%d %H:%M:%S')
                            
                            uid = user.id_str
                            logging.debug("Recording timestamp {} for {}".format(uid, tmstamp))
                            query_str = ("UPDATE users SET last_tweet_before_415 = '{}' "
                                        "WHERE user_id = '{}'").format(tmstamp, uid)
                            logging.debug(query_str)
                            cur.execute(query_str)
                            con.commit()
                            break
                        else:
                            continue
                except tweepy.TweepError as e:
                    if e.message == 'Not authorized':
                        no_requested=3200
    
def lookup_users_do_something(con, api, query, do_something):    
    # get user ids
    user_ids = user_ids_query(con, query)

    pagesize = 100
    while len(user_ids) > 0:
        cur_users = user_ids[:pagesize]
        try:
            user_objs = api.lookup_users(user_ids=cur_users)
            logging.debug(len(user_objs))
        except tweepy.TweepError as e:
            # no matches
            if e.args[0][0]['code'] == 17:
                user_ids = user_ids[pagesize:]
                continue
            print e.message
            print e.args
            raise
        do_something(con, user_objs)
        #break
        user_ids = user_ids[pagesize:]
 


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
    elif sys.argv[1] == 'getprofileinfo':
        with connect_db() as con:
            get_user_name_created_at_and_pic(con, get_api(wait_on_rate_limit=True))
    elif sys.argv[1] == 'gettweetfreq':
        with connect_db() as con:
            lookup_users_do_something(con,
                                      get_api(wait_on_rate_limit=True),
                                      "SELECT user_id FROM users " \
                                      "WHERE last_tweet_at < '2015-04-15' " \
                                      "AND last_tweet_before_415 IS NULL",
                                      get_415_5_tweets)
            
    
    #    fill_in_field(get_api(), con)

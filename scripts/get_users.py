#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import os
import logging
logging.basicConfig(level=logging.WARNING)
import re
import time
import tweepy
import psycopg2 as mdb
from twitter_api_func import get_api
from shared_utilities import normalize_text, connect_db


def get_missing_users(con):
    with con.cursor() as cur:
        cur.execute("SELECT DISTINCT user_id FROM tweets WHERE "
                    "user_id NOT IN (SELECT user_id FROM users)")
        logging.info(cur.rowcount)
        missing_user_ids = zip(*cur.fetchall())[0]
        return missing_user_ids

def add_users(ids, api, con):
    with con.cursor() as cur:
        while len(ids) > 0:
            ids_to_look_up = ids[0:min(len(ids),100)]

            try:
                user_result = api.lookup_users(ids_to_look_up)
            except tweepy.TweepError:
                logging.warning("Twitter error", exc_info=True)
                logging.warning(api.rate_limit_status()['resources'][u'/users/lookup'])
                time.sleep(float(api.rate_limit_status()['resources'][u'/users/lookup']['reset']) - time.time)
                continue
                
            for user in user_result:
                try:
                    logging.debug("Adding user to db...")
                    add_user_to_db(user, cur)
                except mdb.IntegrityError:
                    logging.warning("User already exists")
                    con.rollback()
                con.commit()
            try:
                ids = ids[100:]
            except:
                ids = []

                        
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
                normalize_text(getattr(user,'location',''))))

                     
if __name__ == "__main__":
    with connect_db() as con:
        add_users(list(set(get_missing_users(con))), get_api(), con)

#!/usr/bin/env python
# -*- coding: UTF-8 -*-

import sys
import os
import pprint as pp
import logging
logging.basicConfig(level=logging.DEBUG)
import re
import time
import urllib
import psycopg2 as mdb
import geopy
logging.getLogger("geopy").setLevel(logging.DEBUG)


def connect_db():
    con = mdb.connect("dbname=tweets user=patrick")
    assert con is not None
    return con

def add_geocodes(con, 
    key_file_path=os.path.join(os.path.dirname(sys.argv[0]), "geocode.key"),
    src_tbl="users", src_place_field="place",
    dest_tbl="places",
    dest_place_field="place", 
    dest_lat_field="coded_lat", 
    dest_long_field = "coded_long", min_count=3):
    """For distinct locations in users where > min_count values are
    uncoded, connect to google geocoding API and attempt to geocode them"""
    
    with open(key_file_path) as f:
        geocoding_api_key = f.read()
    # get geocoder
    googeo = geopy.geocoders.GoogleV3(api_key=geocoding_api_key,domain='maps.googleapis.com',scheme='https')
    
    with con.cursor() as cur:
        cur.execute(("SELECT {0}, COUNT({0}) "
                     "as freq FROM {1} WHERE {0} NOT IN (SELECT {2} FROM {3}) "
                     "GROUP BY {0}").format(src_place_field, src_tbl, dest_place_field, dest_tbl))
        distinct_places = [pl for pl, ct in 
                dict(cur.fetchall()).iteritems() 
                if ct >= min_count and pl != "None"]
        
        for place in distinct_places:
            if place.strip() == '':
                continue
            
            try:
                logging.debug(place)
                logging.debug(googeo.format_string)
                logging.debug("Geocoding {}".format(place))
                geocodes = googeo.geocode(place,timeout=7)
                if geocodes == None:
                    logging.warning("Location not found")
                    cur.execute(("INSERT INTO {0} ({1}, {2}, {3}) "
                            "VALUES ('{4}', '{5}', '{6}')").format(
                    dest_tbl,dest_place_field,dest_lat_field,dest_long_field,
                    place,-500.,500.)
                    )
                else:
                    cur.execute(("INSERT INTO {0} ({1}, {2}, {3}) "
                            "VALUES ('{4}', '{5}', '{6}')").format(
                    dest_tbl,dest_place_field,dest_lat_field,dest_long_field,
                    place,geocodes.latitude,geocodes.longitude)
                    )
                con.commit()
            except AttributeError:
                logging.warning("Error", exc_info=True)
                
                con.rollback()
            except geopy.exc.GeocoderQueryError:
                logging.warning("Bad query",exc_info=True)
                con.rollback()
                
            time.sleep(0.5)

            
#def populate_places(con, src_tbl="users", dest_tbl="places"):
    #"""Populate 
    #with con.cursor() as cur:
if __name__ == "__main__":
    print sys.argv[0]
    add_geocodes(connect_db())

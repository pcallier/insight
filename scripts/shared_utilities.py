import psycopg2 as mdb
import re
import logging

def normalize_text(text):
    try:
        blank_these = re.compile(r"(')+")
        space_these = re.compile(r"(\s+)")
        return space_these.sub(" ", blank_these.sub("", text))
    except TypeError:
        logging.debug("Expected text, got {}, type {}".format(text, type(text)))
        return ""
        
        
def connect_db():
    con = mdb.connect("dbname=tweets user=patrick")
    assert con is not None
    return con

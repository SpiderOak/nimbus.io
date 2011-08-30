# -*- coding: utf-8 -*-
"""
util.py

utility routines for space accounting server
"""
import datetime

def floor_hour(raw_datetime):
    """return a datetime rounded to the floor hour"""
    return datetime.datetime(
        year = raw_datetime.year,
        month = raw_datetime.month,
        day = raw_datetime.day,
        hour = raw_datetime.hour,
        minute = 0,
        second = 0,
        microsecond = 0
    )


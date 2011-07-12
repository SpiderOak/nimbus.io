# -*- coding: utf-8 -*-
"""
archiver.py

archive one file
"""

def archive_blob(config, message, body, send_queue):
    """
    archive a blob of data passed as an argument
    """
    status_message = {
        "message-type"  : message["client-topic"],
        "completed"     : True,        
    }
    send_queue.put((status_message, None, ))
        


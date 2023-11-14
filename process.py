import logging
import random
import time
import requests
import os
import pandas as pd
from dv_utils import default_settings, Client, audit_log 

logger = logging.getLogger(__name__)

# let the log go to stdout, as it will be captured by the cage operator
logging.basicConfig(
    level=default_settings.log_level,
    format="%(asctime)s - %(levelname)s - %(message)s",
)

# define an event processing function
def event_processor(evt: dict):
    """
    Process an incoming event
    Exception raised by this function are handled by the default event listener and reported in the logs.
    """
    
    logger.info(f"Processing event {evt}")

    # dispatch events according to their type
    evt_type =evt.get("type", "")
    if(evt_type.startswith("TEST_")):
        audit_log("received a test event", evt=evt_type.removeprefix("TEST_"))

        event_catalonia_crashes()
        # if(random.choice([True, False])):
        #     raise ValueError('This randomly failed')

def event_catalonia_crashes():
   path = "/resources/configuration.json"
   f = open(path)
   data = json.load(f)
   url = data['connectionkeys']['SHARING_URL']

   filelocation="/resources/outputs/output.csv"
   
   logger.info("start")
   logger.info(url)
   crash = urllib.request.urlopen(url);

   with open(filelocation, 'w', newline='') as file:
      for line in crash:
         logger.info(line)
         file.write(line)

   f.close()

import logging
import random
import time
import requests
import os
import json
import urllib.request
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
        logger.info("start_crash")
        crash_event_processor(evt)
        logger.info("stop_crash")

        # if(random.choice([True, False])):
        #     raise ValueError('This randomly failed')

def crash_event_processor(evt: dict):
   try:
      logger.info("start")

      path = "/resources/data/configuration.json"
      # path = "configuration.json"
      f = open(path)
      data = json.load(f)
      f.close()
      print(data)
      url = data['connectorKeys']['SHARING_URL']

      # filelocation="output.csv"
      filelocation="/resources/outputs/output.csv"
      
      logger.info("start")
      logger.info(url)

      crash = urllib.request.urlopen(url);
      content = crash.read().decode('utf-8')

      with open(filelocation, 'w', newline='') as file:
         for line in content:
            file.write(line)

   except Exception as err:
      logger.error(f"Failed processing event: {err}")
      logger.error(err)


if __name__ == "__main__":
   crash_event_processor({"type": "TEST_CRASH"})

import connexion

import yaml
import logging
from logging import config

import json
import requests
import datetime
from datetime import datetime

from apscheduler.schedulers.background import BackgroundScheduler

from pykafka import KafkaClient
from threading import Thread
from flask_cors import CORS, cross_origin

with open('app_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())
    base_url = app_config['eventstore']['url']
    log_seconds = app_config['scheduler']['period_sec']
    log_datastore = app_config['datastore']['filename']

with open('log_conf.yaml', 'r') as f:
    log_config = yaml.safe_load(f.read())
    logging.config.dictConfig(log_config)

with open('kafka_conf.yaml', 'r') as f:
    app_config = yaml.safe_load(f.read())
    kafka_server = app_config['kafka-conf']['kafka-server']
    kafka_port = app_config['kafka-conf']['kafka-port']
    kafka_topic = app_config['kafka-conf']['kafka-topic']

logger = logging.getLogger('basicLogger')
logger.setLevel(logging.DEBUG)


def get_request_stats():

    logger.info("Start of Request")
    with open(log_datastore, "r") as json_file:
        data = json.load(json_file)

    ir = data['num_immediate_requests']
    sr = data['num_scheduled_requests']
    timestamp = data['updated_timestamp']

    stats = {
        'num_immediate_requests': ir,
        'num_scheduled_requests': sr,
        'updated_timestamp': timestamp
    }

    logger.debug(stats)
    logger.info('End of Request')

    return stats, 200


def populate_stats():
    """ Periodically update stats
        Check if file exists, update file with new data (run get query), update timestamp, log at start and end"""

    logger.info("Start Periodic Processing")
    cur_datetime = datetime.now()

    with open(log_datastore, "r") as json_file:
        data = json.load(json_file)

    ir = data['num_immediate_requests']
    sr = data['num_scheduled_requests']
    timestamp = data['updated_timestamp']

    url1 = '{}/request/immediate'.format(base_url)
    params1 = {'startDate': timestamp, 'endDate': cur_datetime}

    url2 = '{}/request/scheduled'.format(base_url)
    params2 = {'startDate': timestamp, 'endDate': cur_datetime}

    g1 = requests.get(url1, params1)
    g2 = requests.get(url2, params2)

    if g1.status_code is not 200 or g2.status_code is not 200:
        logger.error('Did not receive status code 200')
    else:
        response1 = g1.json()
        response2 = g2.json()
        num_events1 = len(response1)
        num_events2 = len(response2)
        msg = 'Immediate Requests: ', ir, 'Scheduled Requests: ', sr
        logger.info(msg)

        if num_events1 > 0:
            ir = ir + num_events1

        if num_events2 > 0:
            sr = sr + num_events2

        data['num_immediate_requests'] = ir
        data['num_scheduled_requests'] = sr
        data['updated_timestamp'] = str(cur_datetime)

        with open(log_datastore, "w") as json_file:
            json_file.write(json.dumps(data))

        logger.info("Period Processing Complete")


def init_scheduler():
    sched = BackgroundScheduler(daemon=True)
    sched.add_job(populate_stats,
                  'interval',
                  seconds=log_seconds)
    sched.start()


if __name__ == '__main__':
    app = connexion.FlaskApp(__name__, specification_dir='')
    CORS(app.app)
    app.add_api('openapi.yaml')
    app.app.config['CORS_HEADERS'] = 'Content-Type'
    init_scheduler()
    app.run(port=8100, use_reloader=False)

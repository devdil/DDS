import config
from functools import wraps
import pika
import threading
import logging

logger = logging.getLogger(__name__)
logger.setLevel(logging.DEBUG)

# create a file handler
handler = logging.FileHandler('scheduler.log')
handler.setLevel(logging.DEBUG)

# create a logging format
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
handler.setFormatter(formatter)

# add the handlers to the logger
logger.addHandler(handler)

RABBIT_MQ_CONFIG = config.RABBIT_MQ_CONFIG;


class Scheduler(object):
    username = RABBIT_MQ_CONFIG['username']
    password = RABBIT_MQ_CONFIG['password']
    host = RABBIT_MQ_CONFIG['host']
    port = RABBIT_MQ_CONFIG['port']
    connected = False
    EXCHANGE_NAME = "scheduler"
    channel=None

    class Listener(object):

        def __init__(self, state, scheduler_state_map):
            self.state_name = state
            self.scheduler_state_map = scheduler_state_map

        def onStateChange(self, ch, method, properties, body):
            #call the callback method
            self.scheduler_state_map[self.state_name](body)


    def __init__(self):
        self.state_map = {}

    @classmethod
    def connect(cls):
        if not cls.connected:
            # connect
            credentials = pika.PlainCredentials(cls.username, cls.password)
            parameters = pika.ConnectionParameters(host=cls.host, port=cls.port, credentials=credentials)
            connection = pika.BlockingConnection(parameters)
            Scheduler.channel = connection.channel()

    def on(self, state_name):
        def func(f):
            # do something with the function
            if not self.state_map.has_key(state_name):
                # save the state name and function in a map
                self.state_map[state_name] = f

            @wraps(f)
            def actual_function(*args, **kwargs):
                # before the the actual function invocation
                # after the actual funtion invocation
                return

            return actual_function

        return func

    def intialize(self):
        Scheduler.connect()
        # initialize all states
        # initialize exchanges and bind the states to the exchangs with routing key as states
        # initialize listener threads to all the states which accpets the function as callback parameters
        Scheduler.channel.exchange_declare(exchange=self.EXCHANGE_NAME,
                                 exchange_type='direct')
        for state in self.state_map.keys():
            queue_name = state
            Scheduler.channel.queue_declare(queue=queue_name, durable=True )
            Scheduler.channel.queue_bind(exchange=Scheduler.EXCHANGE_NAME,
                                         queue=queue_name,
                                         routing_key=queue_name)
        # by this we have registered queues, exchanges and bindings between them.

        # for each state in state_map attach function callbacks for each state by calling rabbitmq basic_consume method
        for state, func in self.state_map.iteritems():
            # for each state change we initalize a Listener object and attachg on stateChange
            # method
            state_change_listener = self.Listener(state, self.state_map)
            Scheduler.channel.basic_consume(state_change_listener.onStateChange, queue=state, no_ack=True)

    def run(self):
        Scheduler.channel.start_consuming()

    def moveTo(self, statename, payload):
            dir(Scheduler.channel)
            Scheduler.channel.basic_publish(exchange=self.EXCHANGE_NAME, routing_key=statename, body=payload)


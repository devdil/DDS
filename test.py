#!/usr/bin/python

from scheduler import scheduler
import json

scheduler = scheduler.Scheduler()


class MyScheduler(object):

    @scheduler.on("new")
    def onNew(body):
        print "state: new   received {}".format(body)
        scheduler.moveTo("running", json.dumps(body))

    @scheduler.on("running")
    def onNew(body):
        print "state: running   received {}".format(body)
        scheduler.moveTo("completed", json.dumps(body))

    @scheduler.on("completed")
    def onNew(body):
        print "state: completed   received {}".format(body)

    def initialize(self):
        payload = {"name": "This is a payload"}
        scheduler.moveTo("new", json.dumps(payload))

scheduler.intialize()
myscheduler = MyScheduler()
myscheduler.initialize()

scheduler.run()



#!/usr/bin/env python
# coding: utf-8

from logwatcher import LogKey

class EntryStatus():
    scheduled = 0
    executing = 1
    completed = 2

class ScheduleEntry():
    def __init__(self, job=None, host=None, sched_start=None, sched_end=None, condor_id=None):
        self.job = job # if job is None, it's a vm boot entry
        self.host = host
        self.condor_id = condor_id
        self.status = EntryStatus.scheduled
        self.log = {}
        self.log[LogKey.sched_start] = sched_start
        self.log[LogKey.sched_end] = sched_end
        self.log[LogKey.real_start] = None
        self.log[LogKey.real_end] = None

    def start(self):
        if self.log[LogKey.real_start] != None:
            return self.log[LogKey.real_start]
        else:
            return self.log[LogKey.sched_start]
    
    def end(self):
        if self.log[LogKey.real_end] != None:
            return self.log[LogKey.real_end]
        elif self.log[LogKey.real_start] != None:
            return self.log[LogKey.real_start] + self.job.pduration
        else:
            return self.log[LogKey.sched_end]
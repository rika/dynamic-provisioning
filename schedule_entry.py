#!/usr/bin/env python
# coding: utf-8

class ScheduleEntry():
    def __init__(self, job, machine, start, end):
        self.job = job # if job is None, it's a vm boot entry
        self.machine = machine
        self.status = EntryStatus.scheduled
        self.sched_start = start
        self.sched_end = end
        self.real_start = None
        self.real_end = None

    def start(self):
        if self.real_start != None:
            return self.real_start
        else:
            return self.sched_start
    
    def end(self):
        if self.real_end != None:
            return self.real_end
        elif self.real_start != None:
            return self.real_start + self.job.pduration
        else:
            return self.sched_end


class EntryStatus():
    scheduled = 0
    executing = 1
    completed = 2
 
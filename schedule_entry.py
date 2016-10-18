#!/usr/bin/env python
# coding: utf-8

class ScheduleEntry():
    def __init__(self, job, machine, start, end):
        self.job = job
        self.job_status = JobStatus.scheduled
        self.machine = machine
        self.sched_start = start
        self.sched_end = end 
        self.real_start = None
        self.real_end = None
    
#    def __str__(self):
#        return "%s @ %s from: %.2f to: %.2f" % (self.job, self.machine, self.start, self.end)


class JobStatus():
    scheduled = 0
    executing = 1
    completed = 2
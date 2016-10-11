#!/usr/bin/env python
# coding: utf-8

class ScheduleEntry():
    def __init__(self, job, machine, start, end):
        self.job = job
        self.machine = machine
        self.start = start
        self.end = end
    
    def __str__(self):
        return "%s @ %s from: %.2f to: %.2f" % (self.job, self.machine, self.start, self.end)

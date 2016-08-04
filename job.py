#!/usr/bin/env python
# coding: utf-8

class Job:
    def __init__(self, id):
        self.id = id
        self.children = []
        self.parents = []
        
        self.pduration = 1 # default
        self.sched_entry = None
        self.rank = 0
        
    def parent_of(self, job):
        self.children.append(job)
        job.parents.append(self)
    
    def __str__(self):
        return "JOB %s [%.2f] (%.2f)" % (self.id, self.rank, self.pduration)
    
    # necessário para a ordenação do heapq
    def __lt__(self, job2):
        return (self.pduration < job2.pduration)
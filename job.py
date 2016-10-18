#!/usr/bin/env python
# coding: utf-8

class Job:
    def __init__(self, id, wf_id):
        self.id = id
        self.wf_id = wf_id
        self.children = []
        self.parents = []
        
    def parent_of(self, job):
        self.children.append(job)
        job.parents.append(self)
    
    def __str__(self):
        return "JOB %s %s (%.2f)" % (self.id, self.wf_id, self.pduration)
    
    '''
    # necessário para a ordenação do heapq
    def __lt__(self, job2):
        return (self.pduration < job2.pduration)
    '''
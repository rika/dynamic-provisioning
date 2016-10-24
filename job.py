#!/usr/bin/env python
# coding: utf-8

class Job:
    def __init__(self, id, name, wf_id):
        self.id = id
        self.name = name
        self.global_id = None
        self.wf_id = wf_id
        self.children = []
        self.parents = []
        
    def parent_of(self, job):
        self.children.append(job)
        job.parents.append(self)
    

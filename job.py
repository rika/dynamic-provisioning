#!/usr/bin/env python
# coding: utf-8

class Job:
    def __init__(self, dag_job_id, wf_id):
        self.dag_job_id = dag_job_id
        self.wf_id = wf_id
        self.parents = []
        self.children = []
        self.pduration = None

    def is_pegasus_job(self):
        if self.dag_job_id.startswith('create_dir') or \
           self.dag_job_id.startswith('register') or \
           self.dag_job_id.startswith('stage_in') or \
           self.dag_job_id.startswith('stage_out') or \
           self.dag_job_id.startswith('stage_worker'):
            return True
        else:
            return False
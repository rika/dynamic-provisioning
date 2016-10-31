#!/usr/bin/env python
# coding: utf-8

import os
from datetime import datetime
JOBSTATE_FILENAME = 'jobstate.log'

'''
 Log structured

1. The ISO timestamp for the time at which the particular event happened.
2. The name of the job.
3. The event recorded by DAGMan for the job.
4. The condor id of the job in the queue on the submit node.
5. The pegasus site to which the job is mapped.
6. The job time requirements from the submit file.
7. The job submit sequence for this workflow.
'''
L_TIME=0
L_NAME=1
L_EVENT=2
L_ID=3
L_SITE=4
L_REQ=5
L_SEQ=6

'''
Events

SUBMIT                  job is submitted by condor schedd for execution.
EXECUTE                 condor schedd detects that a job has started execution.
GLOBUS_SUBMIT           the job has been submitted to the remote resource. It's only written for GRAM jobs (i.e. gt2 and gt4).
GRID_SUBMIT             same as GLOBUS_SUBMIT event. The ULOG_GRID_SUBMIT event is written for all grid universe jobs./
JOB_TERMINATED          job terminated on the remote node.
JOB_SUCCESS             job succeeded on the remote host, condor id will be zero (successful exit code).
JOB_FAILURE             job failed on the remote host, condor id will be the job's exit code.
POST_SCRIPT_STARTED     post script started by DAGMan on the submit host, usually to parse the kickstart output
POST_SCRIPT_TERMINATED  post script finished on the submit node.
POST_SCRIPT_SUCCESS     post script succeeded.
POST_SCRIPT_FAILURE     post script failed.
'''

class LogKey():
    # provisioner events
    sched_start = '_SCHED_START'
    sched_end = '_SCHED_END'
    real_start = '_REAL_START'
    real_end = '_REAL_END'
    
    # condor/pegasus events
    submit = 'SUBMIT'                  # job is submitted by condor schedd for execution.
    execute = 'EXECUTE'                # condor schedd detects that a job has started execution.
    globus_submit = 'GLOBUS_SUBMIT'    # the job has been submitted to the remote resource. It's only written for GRAM jobs (i.e. gt2 and gt4).
    grid_submit = 'GRID_SUBMIT'        # same as GLOBUS_SUBMIT event. The ULOG_GRID_SUBMIT event is written for all grid universe jobs./
    job_terminated = 'JOB_TERMINATED'  # job terminated on the remote node.
    job_success = 'JOB_SUCCESS'        # job succeeded on the remote host, condor id will be zero (successful exit code).
    job_failure = 'JOB_FAILURE'        # job failed on the remote host, condor id will be the job's exit code.
    post_script_started = 'POST_SCRIPT_STARTED'       # post script started by DAGMan on the submit host, usually to parse the kickstart output
    post_script_terminated = 'POST_SCRIPT_TERMINATED' # post script finished on the submit node.
    post_script_success = 'POST_SCRIPT_SUCCESS'       # post script succeeded.
    post_script_failure = 'POST_SCRIPT_FAILURE'       # post script failed.

class LogEntry():
    def __init__(self, line):
        keys = line.split()
        self.timestamp = datetime.fromtimestamp(float(keys[L_TIME]))
        self.event = keys[L_EVENT]
        self.name = keys[L_NAME]
        self.id = keys[L_ID].split('.')[0] 
        self.site = keys[L_SITE]
        self.req = keys[L_REQ]
        self.seq = keys[L_SEQ]
         
class LogWatcher():
    def __init__(self):
        self.fps = []
        self.waiting = []
    
    def add(self, wf_dir):
        path = os.path.join(wf_dir, JOBSTATE_FILENAME)
        if os.path.isfile(path):
            self.fps.append(open(path,'r'))
        else:
            self.waiting.append(path)
    
    def nexts(self):
        # add waiting path
        ready = [path for path in self.waiting if os.path.isfile(path)]
        if len(ready) > 0:
            self.waiting = [path for path in self.waiting if path not in ready]
            for path in ready:
                self.fps.append(open(path,'r'))
        
        # get new lines
        entries = []
        done = []
        for fp in self.fps:
            for line in fp.readlines():
                if 'INTERNAL' in line:
                    if 'FINISHED' in line:
                        done.append(fp)
                else:
                    entry = LogEntry(line)
                    if entry.id != '0':
                        entries.append(entry)
        self.fps = [fp for fp in self.fps if fp not in done]
        return entries
    
    def watching_none(self):
        return (len(self.fps) == 0)
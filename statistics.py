#!/usr/bin/env python
# coding: utf-8

import os
import csv

from schedule_entry import EntryStatus
from machine import MachineStatus

def dump_stat(path, data, headers):
    with open(path, 'w') as out:
        csv_out = csv.writer(out)
        csv_out.writerow(headers)
        for row in data:
            csv_out.writerow(row)

class Statistics():
    def __init__(self):
        self.numbers = []
        self.scheds = []
        self.entries = []
        self.durations = [] 
        
    def snapshot(self, provisioner):
        entries = provisioner.schedule.entries 
        
        # Number of jobs in scheduled/execution
        njs = len([e for e in entries if e.status == EntryStatus.scheduled])
        nje = len([e for e in entries if e.status == EntryStatus.executing])
        
        # Number of machines allocating/running
        nma = len([m for m in provisioner.machines if m.status == MachineStatus.allocating])
        nmr = len([m for m in provisioner.machines if m.status == MachineStatus.running])
        
        self.numbers.append((provisioner.timestamp, njs, nje, nma, nmr))

    def schedshot(self, provisioner):
        self.scheds.append((provisioner.timestamp, provisioner.budget, provisioner.cost_pred, provisioner.wf_end))
        
    def jobs(self, entries):
        for e in entries:
            if e.host != None:
                host_id = e.host.id
                condor_slot = e.host.condor_slot
            else:
                host_id = condor_slot = None
            
            if e.job != None:
                wf_id = e.job.wf_id
                dag_job_id = e.job.dag_job_id
            else:
                wf_id = dag_job_id = None
            
            for event in e.log.keys():
                if e.log[event]:
                    self.entries.append((host_id, condor_slot, wf_id, dag_job_id, e.condor_id, event, e.log[event]))
            
            d = {}
            if dag_job_id and 'EXECUTE' in e.log.keys() and 'JOB_TERMINATED' in e.log.keys() and 'SUBMIT' in e.log.keys():
                parts = dag_job_id.split('_')
                if len(parts) == 2: 
                    jt = parts[0]
                    d[jt] = [
                        (d[jt][0] if jt in d.keys() else 0) +1,
                        (d[jt][1] if jt in d.keys() else 0) +(e.log['JOB_TERMINATED'] - e.log['EXECUTE']).total_seconds(),
                        (d[jt][2] if jt in d.keys() else 0) +(e.log['EXECUTE']        - e.log['SUBMIT']).total_seconds(),
                        (d[jt][3] if jt in d.keys() else 0) +(e.log['JOB_TERMINATED'] - e.log['SUBMIT']).total_seconds(),
                    ]
            for jt in d.keys(): 
                self.durations.append((jt, d[jt][1]*1.0 / d[jt][0], d[jt][2]*1.0 / d[jt][0], d[jt][3]*1.0 / d[jt][0]))
            
    def dump(self):
        home = os.path.expanduser('~')
        directory = os.path.join(home, '.dynamic_provisioning')
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        print 'Writing statistics in ' + str(directory)
        
        path = os.path.join(directory, 'numbers.csv')
        headers = ['timestamp','n_jobs_s','n_jobs_e','n_machines_a','n_machines_r']
        dump_stat(path, self.numbers, headers)
                
        path = os.path.join(directory, 'budget.csv')
        headers = ['timestamp', 'budget', 'cost_prediction', 'wf_end']
        dump_stat(path, self.scheds, headers)

        path = os.path.join(directory, 'jobs.csv')
        headers = ['host', 'slot', 'workflow', 'dag_job_id','condor_id', 'event', 'timestamp']
        dump_stat(path, self.entries, headers)

        path = os.path.join(directory, 'durations.csv')
        headers = ['job', 'execute_time', 'queue_time', 'total_time']
        dump_stat(path, self.durations, headers)
        
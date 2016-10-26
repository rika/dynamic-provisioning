
import os
import csv
from datetime import datetime

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
        self.created_at = datetime.now()
        self.numbers = []
        self.scheds = []
        self.entries = []
        
    def snapshot(self, provisioner):
        entries = {x for v in provisioner.entries.itervalues() for x in v} 
        
        # Number of jobs in execution
        nj = len([e for e in entries if e.status == EntryStatus.executing])
        
        # Number of machines
        nma = len([m for m in provisioner.machines if m.status == MachineStatus.allocating])
        nmr = len([m for m in provisioner.machines if m.status == MachineStatus.running])
        
        timestamp = (datetime.now() - self.created_at).seconds
        self.numbers.append((timestamp, nj, nma, nmr))

    def schedshot(self, budget, cost, wf_end):
        timestamp = (datetime.now() - self.created_at).seconds
        timespan = (wf_end-self.created_at).seconds
        self.scheds.append((timestamp, budget, cost, timespan))
        
    def jobs(self, provisioner):
        for m,l in provisioner.entries.iteritems():
            for e in l:
                self.entries.append((m.id, m.condor_slot, e.job.wf_id, e.job.id, e.job.name,  \
                                     (self.created_at - e.real_start).seconds, \
                                     (self.created_at - e.real_end).seconds, \
                                     (self.created_at - e.sched_start).seconds, \
                                     (self.created_at - e.sched_end).seconds))
     
    def dump(self):
        home = os.path.expanduser('~')
        directory = os.path.join(home, '.dynamic_provision')
        if not os.path.exists(directory):
            os.makedirs(directory)
        
        print 'Writing statistics in ' + str(directory)
        
        path = os.path.join(directory, 'numbers.csv')
        headers = ['timestamp','n_jobs','n_machines_a','n_machines_r']
        dump_stat(path, self.numbers, headers)
                
        path = os.path.join(directory, 'budget.csv')
        headers = ['timestamp', 'budget', 'cost_prediction', 'timespan']
        dump_stat(path, self.scheds, headers)

        path = os.path.join(directory, 'jobs.csv')
        headers = ['machine', 'slot', 'workflow', 'job_id', 'job_name', 'real_start', 'real_end', 'sched_start', 'sched_end']
        dump_stat(path, self.entries, headers)
        

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
        self.numbers = []
        self.scheds = []
        self.entries = []
        
    def snapshot(self, timestamp, provisioner):
        entries = {x for v in provisioner.entries.itervalues() for x in v} 
        
        # Number of jobs in execution
        nj = len([e for e in entries if e.status == EntryStatus.executing])
        
        # Number of machines
        nma = len([m for m in provisioner.machines if m.status == MachineStatus.allocating])
        nmr = len([m for m in provisioner.machines if m.status == MachineStatus.running])
        
        self.numbers.append((timestamp, nj, nma, nmr))

    def schedshot(self, timestamp, budget, cost, wf_end):
        self.scheds.append((timestamp, budget, cost, wf_end))
        
    def jobs(self, entries):
        for e in entries:
            if e.real_end == None or e.real_start == None:
                duration = None
            else:
                duration = (e.real_end - e.real_start).seconds
                
            if e.machine != None:
                machine_id = e.machine.id
                condor_slot = e.machine.condor_slot
            else:
                machine_id = condor_slot = None
                
            self.entries.append((machine_id, condor_slot, e.job.wf_id, e.job.dag_job_id,  \
                                 e.sched_start, \
                                 e.sched_end, \
                                 e.real_start, \
                                 e.real_end, \
                                 duration))
     
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
        headers = ['timestamp', 'budget', 'cost_prediction', 'wf_end']
        dump_stat(path, self.scheds, headers)

        path = os.path.join(directory, 'jobs.csv')
        headers = ['machine', 'slot', 'workflow', 'dag_job_id', 'sched_start', 'sched_end', 'real_start', 'real_end', 'duration']
        dump_stat(path, self.entries, headers)
        
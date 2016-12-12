#!/usr/bin/env python
# coding: utf-8

from datetime import datetime

from job import Job
from machine import Machine
from machine import MachineStatus
from workflow import Workflow
from schedule_entry import ScheduleEntry 
from schedule_entry import EntryStatus
from condor import condor_slots
from condor import condor_history
from logwatcher import LogWatcher, LogKey

class Monitor():
    def __init__(self):
        self.workflow = Workflow()
        self.creation_timestamp = self.timestamp = datetime.now()
        self.logwatcher = LogWatcher()
        
        manager = Machine()
        manager.status = MachineStatus.manager
        manager.condor_slot = 'local'
        self.machines = [manager]
        
        boot_entry = ScheduleEntry(Job('boot', None), manager, None, None)
        boot_entry.real_start = self.timestamp
        boot_entry.real_end = self.timestamp
        boot_entry.status = EntryStatus.completed
        self.entries = [boot_entry]
        self.entries_cid = {}
        
    def add_workflow(self, workflow_dir):
        wf_id = self.workflow.add_workflow(workflow_dir)
        self.logwatcher.add(wf_id, workflow_dir)
            
    def sync_machines(self):
        slots = condor_slots()
        for s in slots:
            if s not in [m.condor_slot for m in self.machines]:
                machine = Machine()
                machine.status = MachineStatus.running
                machine.condor_slot = s
                boot_job = Job('boot', None)
                boot_entry = ScheduleEntry(boot_job, machine, None, None)
                boot_entry.log[LogKey.real_start] = self.creation_timestamp
                boot_entry.log[LogKey.real_end] = self.timestamp
                boot_entry.status = EntryStatus.completed
                self.entries.append(boot_entry)
                self.machines.append(machine)
                print "++Machine", s
                
    def sync_jobs(self):
        log_entries = self.logwatcher.nexts()
        for le in log_entries:
            if le.id in self.entries_cid: # in dict keys
                entry = self.entries_cid[le.id]
            else:
                entry = ScheduleEntry(condor_id=le.id)
                self.entries.append(entry)
                self.entries_cid[le.id] = entry
                print "++Job", le.id
                
            entry.log[le.event] = le.timestamp
            
            if le.event == LogKey.execute:
                entry.status = EntryStatus.executing
            elif le.event == LogKey.job_terminated:
                entry.status = EntryStatus.completed
                wf_id, dag_job_id, slot = condor_history(le.id)
                
                job = next((j for j in self.workflow.jobs if j.dag_job_id == dag_job_id and j.wf_id == wf_id), None)
                if job:
                    entry.job = job
                    entry.host = next((m for m in self.machines if m.condor_slot == slot), self.machines[0])
                    print "--Job", le.id, dag_job_id, entry.host.condor_slot
            
    def update_timestamp(self):
        self.timestamp = datetime.now()
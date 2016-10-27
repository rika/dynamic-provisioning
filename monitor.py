
from datetime import datetime

from job import Job
from machine import Machine
from machine import MachineStatus
from workflow import Workflow
from schedule_entry import ScheduleEntry
from schedule_entry import EntryStatus
from condor import condor_slots
from condor import condor_q
from condor import condor_job_completed
from provisioner import sync_parents

class Monitor():
    def __init__(self):
        self.workflow = Workflow()
        self.creation_timestamp = self.timestamp = datetime.now()
        
        manager = Machine()
        manager.status = MachineStatus.manager
        manager.condor_slot = 'local'
        self.machines = [manager]
        
        boot_entry = ScheduleEntry(Job('boot', None), manager, None, None)
        boot_entry.real_start = self.timestamp
        boot_entry.real_end = self.timestamp
        boot_entry.status = EntryStatus.completed
        self.entries = [boot_entry]
        
    def add_workflow(self, workflow_dir):
        self.workflow.add_workflow(workflow_dir)
        for job in self.workflow.jobs:
            self.entries.append(ScheduleEntry(job, None, None, None))
            
    def sync_machines(self):
        slots = condor_slots()
        for s in slots:
            if s not in [m.condor_slot for m in self.machines]:
                machine = Machine()
                machine.status = MachineStatus.running
                machine.condor_slot = s
                boot_job = Job('boot', None)
                boot_entry = ScheduleEntry(boot_job, machine, None, None)
                boot_entry.real_start = self.creation_timestamp
                boot_entry.real_end = self.timestamp
                boot_entry.status = EntryStatus.completed
                self.entries.append(boot_entry)
                self.machines.append(machine)
                print "++Machine", machine, str(s)
                
    def sync_jobs(self):
        lines = condor_q(2) # running jobs
        nq = len(lines)
        ns = len([e for e in self.entries if e.status == EntryStatus.scheduled])
        ne = len([e for e in self.entries if e.status == EntryStatus.executing])
        nc = len([e for e in self.entries if e.status == EntryStatus.completed])
        print '[Q: %d S: %d E: %d C: %d]' % (nq,ns,ne,nc)
        for l in lines:
            global_id, wf_id, dag_job_id, host = l.split(" ")
            machines = [m for m in self.machines if m.condor_slot == host]
            if len(machines) > 0:
                machine = machines[0]
            else:
                machine = self.machines[0] #manager
                
            for entry in [e for e in self.entries if e.job.dag_job_id == dag_job_id \
                                                    and e.job.wf_id == wf_id]:
                entry.status = EntryStatus.executing
                entry.job.global_id = global_id
                entry.real_start = self.timestamp
                entry.machine = machine
                print "++Job", dag_job_id
                sync_parents(e.job, self.entries, self.timestamp)
        
        # completed jobs
        for e in [e for e in self.entries if e.status == EntryStatus.executing]:
            if condor_job_completed(e.job.global_id, e.job.wf_id, e.job.dag_job_id):
                e.status = EntryStatus.completed
                if e.real_start == None:
                    e.real_start = self.timestamp
                e.real_end = self.timestamp
                print "--Job", e.job.dag_job_id
    
    def update_timestamp(self):
        self.timestamp = datetime.now()
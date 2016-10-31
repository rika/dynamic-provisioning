#!/usr/bin/env python
# coding: utf-8

from datetime import datetime

from workflow import Workflow
from machine import Machine
from machine import MachineStatus
from job import Job
from schedule_entry import ScheduleEntry
from schedule_entry import EntryStatus

from condor import condor_slots
from condor import condor_history
from condor import condor_q
from condor import condor_idle
from condor import condor_qedit
from condor import condor_reschedule
from logwatcher import LogWatcher, LogKey
from schedule import sched_number_of_machines, get_nmax, Schedule

from common import VM_COST_PER_SEC

class Provisioner():
    def __init__(self, vm_limit=32):
        self.vm_limit = vm_limit # user input
        self.budget = 0
        self.timestamp = datetime.now()
        
        self.workflow = Workflow()
        self.logwatcher = LogWatcher()
        
        self.schedule = Schedule()
        
        manager = Machine()
        manager.status = MachineStatus.manager
        manager.condor_slot = 'local'
        self.machines = [manager]
        
        boot_entry = ScheduleEntry(Job('boot', None), manager, self.timestamp, self.timestamp)
        boot_entry.real_start = self.timestamp
        boot_entry.real_end = self.timestamp
        boot_entry.status = EntryStatus.completed
        self.schedule.add_entry_host(boot_entry, manager)        
        
    def add_workflow(self, workflow_dir, prediction_file, budget):
        self.budget = self.budget + int(budget)
        self.workflow.add_workflow(workflow_dir, prediction_file=prediction_file)
        self.logwatcher.add(workflow_dir)
            
    def update_schedule(self):
        self.update_budget_timestamp()
        
        # completed and running entries will not change
        self.schedule.rm_scheduled_entries()

        # Max number of vms
        nmax = get_nmax(self.workflow, self.machines, self.schedule, self.vm_limit, self.timestamp)
        
        # Get the number of machines to be used
        schedule, _cost, _n = sched_number_of_machines(self.workflow, self.machines, self.schedule, nmax, self.timestamp, self.budget)
                     
        # Update schedule
        self.schedule = schedule

    def update_budget_timestamp(self):
        timestamp = datetime.now()
        if self.timestamp != None:
            # Supondo vm_cost em cost/second
            # Supondo que não houve mudança no número de máquinas
            # desde o ultimo self.timestamp
            delta = (timestamp - self.timestamp).seconds
            charged = delta * len(self.machines) * VM_COST_PER_SEC
            self.budget = self.budget - charged
        self.timestamp = timestamp
        

    def allocate_new_vms(self):
        # boot entries
        if self.schedule != None:
            for m in self.schedule.entries_host.keys():
                entry = self.schedule.entries_host[m][0]
                if entry.status == EntryStatus.scheduled and entry.start() <= self.timestamp:
                    # allocate vm TODO
                    print "Allocation", str(m)
                    
                    # update machine list
                    self.machines.append(m)
                    # update machine
                    m.status = MachineStatus.allocating
                    # update entry
                    entry.status = EntryStatus.executing
                    entry.log[LogKey.real_start] = self.timestamp
        
    
    def deallocate_vms(self):
        for m in self.machines:
            if m.status == MachineStatus.manager:
                continue
            
            # if there's no more budget or
            # if there's nothing executing or scheduled to the machine
            if self.schedule == None or len([e for e in self.schedule.entries_host[m] if e.status != EntryStatus.completed]) == 0:
                # deallocated machine TODO
                # file transfers?
                print "Deallocation" , str(m)
                
                #update machine
                m.status = MachineStatus.deallocating
                
                print "--Machine", str(m)
                
        # update machine list
        self.machines = [m for m in self.machines if m.status != MachineStatus.deallocating]
    
    
    def sync_machines(self):
        slots = condor_slots()
        running_machines = [m for m in self.machines if m.status == MachineStatus.running]
        allocating_machines = [m for m in self.machines if m.status == MachineStatus.allocating]
        allocating_machines.sort(key=lambda x: self.schedule.entries_host[x][0].start())
        i = 0
        for s in slots:
            if s not in [m.condor_slot for m in running_machines]:
                if len(allocating_machines[i:]) > 0:
                    # update machine
                    allocated_machine = allocating_machines[i]
                    allocated_machine.status = MachineStatus.running
                    allocated_machine.condor_slot = s
                    
                    # update entry
                    boot_entry = self.schedule.entries_host[allocated_machine][0]
                    boot_entry.log[LogKey.real_end] = self.timestamp
                    boot_entry.status = EntryStatus.completed
                
                    i += 1
                    print "++Machine", str(allocated_machine)

    
    def sync_jobs(self):
        scheduled_entries = [e for e in self.schedule.entries if e.status == EntryStatus.scheduled]
        
        idle_cjobs = condor_idle() # idle jobs
        nq = len(idle_cjobs)
        ns = len([e for e in self.schedule.entries if e.status == EntryStatus.scheduled])
        ne = len([e for e in self.schedule.entries if e.status == EntryStatus.executing])
        nc = len([e for e in self.schedule.entries if e.status == EntryStatus.completed])
        print '[Q: %d S: %d E: %d C: %d]' % (nq,ns,ne,nc)

        need_resched = False
        for cjob in idle_cjobs:
            condor_id, wf_id, dag_job_id = cjob.split()
            entry = next((e for e in scheduled_entries if e.job.dag_job_id == dag_job_id \
                                                        and e.job.wf_id == wf_id ), None)
            if entry:
                # if the target machine is ready
                if entry.host.status == MachineStatus.running:
                    entry.condor_id = condor_id
                    entry.status = EntryStatus.executing
                    entry.log[LogKey.real_start] = self.timestamp
                    self.schedule.add_entry_cid(entry, condor_id) 
                    
                    condor_qedit(condor_id, wf_id, dag_job_id, entry.host.condor_slot)
                    need_resched = True
                    print "++Job", dag_job_id
                    
        if need_resched:
            condor_reschedule()

        # Events
        log_entries = self.logwatcher.nexts()
        
        for le in log_entries:
            if le.id in self.schedule.entries_cid:
                entry = self.schedule.entries_cid[le.id]
            else:
                result = condor_q(le.id) or condor_history(le.id)
                if result:
                    wf_id, dag_job_id, _slot = result
                    entry = next((e for e in self.schedule.entries if e.job.dag_job_id == dag_job_id and e.job.wf_id == wf_id), None)
                    if entry:
                        entry.condor_id = le.id
                        self.schedule.add_entry_cid(entry, le.id)
            
            entry.log[le.event] = le.timestamp
            
            if le.event == LogKey.execute:
                entry.status = EntryStatus.executing
                entry.log[LogKey.real_start] = self.timestamp
            
            if le.event == LogKey.post_script_terminated:
                entry.status = EntryStatus.completed 
                entry.log[LogKey.real_end] = self.timestamp
                print "--Job", le.id, entry.job.dag_job_id, entry.host.condor_slot

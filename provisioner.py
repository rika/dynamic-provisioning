#!/usr/bin/env python
# coding: utf-8

import socket
from datetime import datetime

from workflow import Workflow
from machine import Machine
from machine import MachineStatus
from job import Job
from schedule_entry import ScheduleEntry
from schedule_entry import EntryStatus

from condor import condor_slots
from condor import condor_idle
from condor import condor_qedit
from condor import condor_reschedule
from logwatcher import LogWatcher, LogKey
from schedule import sched_number_of_machines, get_nmax, Schedule,\
    sched_cost_pred
from precip.experiment import AzureExperiment

from common import VM_COST_PER_SEC

SCHED_TIMEOUT = 30 # seconds

class Provisioner():
    def __init__(self, vm_limit, azure_config, skip_setup, local):
        self.vm_limit = vm_limit # user input
        self.budget = 0
        self.timestamp = datetime.now()
        self.cost_pred = 0
        self.wf_end = None
        
        self.jobs_terminated = False
        self.last_resched = None
        
        self.workflow = Workflow()
        self.logwatcher = LogWatcher()
        
        self.schedule = Schedule()
        
        manager = Machine()
        manager.status = MachineStatus.manager
        manager.condor_slot = 'manager'
        self.machines = [manager]
        
        boot_entry = ScheduleEntry(Job('boot', None), manager, self.timestamp, self.timestamp)
        boot_entry.real_start = self.timestamp
        boot_entry.real_end = self.timestamp
        boot_entry.status = EntryStatus.completed
        self.schedule.add_entry_host(boot_entry, manager)
        
        self.local = local
        if azure_config and not local:
            hostname = socket.gethostname()
            self.exp = AzureExperiment(azure_config, skip_setup=skip_setup, name=hostname)
            self.master_addr = socket.gethostbyname(hostname)
            self.user = azure_config.admin_username
        else:
            self.exp = self.master_addr = self.user = None
        
    def add_workflow(self, workflow_dir, prediction_file, budget):
        self.budget = self.budget + int(round(float(budget)))
        wf_id = self.workflow.add_workflow(workflow_dir, prediction_file=prediction_file)
        self.logwatcher.add(wf_id, workflow_dir)
            
    def update_schedule(self):
        print 'UPDATE SCHED'
        self.update_budget_timestamp()
        self.last_resched = self.timestamp 
    
        # completed and running entries will not change
        self.schedule.rm_scheduled_entries()

        if self.workflow.has_jobs_to_sched(self.schedule):
            # Max number of vms
            nmax = get_nmax(self.workflow, self.machines, self.schedule, self.vm_limit, self.timestamp, self.local)

            # Get the number of machines to be used
            schedule, _cost, _n = sched_number_of_machines(self.workflow, self.machines, self.schedule, nmax, self.timestamp, self.budget, self.local)
            print "N", _n
            
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
        
    def update_wf_pred(self):
        self.cost_pred, self.wf_end = sched_cost_pred(self.machines, self.schedule, self.timestamp)

    def allocate_new_vms(self):
        # boot entries
        if self.schedule != None:
            for m in self.schedule.entries_host.keys():
                entry = self.schedule.entries_host[m][0]
                if entry.status == EntryStatus.scheduled and entry.start() <= self.timestamp:
                    m.allocate(self.exp, self.master_addr, self.user)
                    
                    self.machines.append(m)
                    entry.status = EntryStatus.executing
                    entry.log[LogKey.real_start] = self.timestamp
        
    
    def deallocate_vms(self):
        for m in self.machines:
            if m.status == MachineStatus.manager:
                continue
            
            # if there's no more budget or
            # if there's nothing executing or scheduled to the machine
            if self.schedule == None or len([e for e in self.schedule.entries_host[m] if e.status != EntryStatus.completed]) == 0:
                m.deallocate(self.exp)
                print "--Machine", m.condor_slot
                
        # update machine list
        self.machines = [m for m in self.machines if m.status != MachineStatus.deallocating]
    
    
    def sync_machines(self):
        slots_addrs = condor_slots()
        running_machines = [m for m in self.machines if m.status == MachineStatus.running]
        allocating_machines = [m for m in self.machines if m.status == MachineStatus.allocating]
        #allocating_machines.sort(key=lambda x: self.schedule.entries_host[x][0].start())
        i = 0
        for (slot,addr) in slots_addrs:
            if slot not in [m.condor_slot for m in running_machines]:
                allocated_machine = None
                if not self.local:
                    allocated_machine = next((m for m in allocating_machines if m.priv_addr == addr), None)
                elif len(allocating_machines[i:]) > 0:
                    # update machine
                    allocated_machine = allocating_machines[i]
                
                if allocated_machine:
                    allocated_machine.status = MachineStatus.running
                    allocated_machine.condor_slot = slot
                    
                    # update entry
                    boot_entry = self.schedule.entries_host[allocated_machine][0]
                    boot_entry.log[LogKey.real_end] = self.timestamp
                    boot_entry.status = EntryStatus.completed
                
                    i += 1
                    print "++Machine", allocated_machine.condor_slot
                else:
                    print "NOT FOUND", slot, addr, [(m.priv_addr,m.status) for m in self.machines]
                

    
    def _handle_log_events(self):
        jobs_terminated = False
        log_entries = self.logwatcher.nexts()
        
        for le in log_entries:
            if le.id in self.schedule.entries_cid:
                sched_entry = self.schedule.entries_cid[le.id]
            else:
                sched_entry = next((e for e in self.schedule.entries if e.job.dag_job_id == le.name and e.job.wf_id == le.wf_id), None)
                if sched_entry:
                    sched_entry.condor_id = le.id
                    self.schedule.add_entry_cid(sched_entry)
            if sched_entry:
                sched_entry.log[le.event] = le.timestamp
                
                if le.event == LogKey.execute:
                    sched_entry.status = EntryStatus.executing
            
                elif le.event == LogKey.job_terminated:
                    sched_entry.status = EntryStatus.completed 
                    sched_entry.log[LogKey.real_end] = self.timestamp
                    print "--Job", le.id, sched_entry.job.dag_job_id, sched_entry.host.condor_slot
                    jobs_terminated = True
            else:
                print 'could not find sched_entry for:', le.id
        return jobs_terminated
                
    def _handle_ready_jobs(self):    
        need_condor_resched = False
        idle_cjobs = condor_idle() # idle jobs

        for cjob in idle_cjobs:
            condor_id, wf_id, dag_job_id = cjob.split()
            if condor_id in self.schedule.entries_cid:
                sched_entry = self.schedule.entries_cid[condor_id]
            else:
                sched_entry = next((e for e in self.schedule.entries \
                                    if e.job.dag_job_id == dag_job_id \
                                    and e.job.wf_id == wf_id ), None)
                sched_entry.condor_id = condor_id
                self.schedule.add_entry_cid(sched_entry)

            if sched_entry and sched_entry.status == EntryStatus.scheduled \
                    and sched_entry.host.status == MachineStatus.running:
                sched_entry.status = EntryStatus.executing
                sched_entry.log[LogKey.real_start] = self.timestamp
                print "++Job", condor_id, dag_job_id, sched_entry.host.condor_slot
                condor_qedit(condor_id, wf_id, dag_job_id, sched_entry.host.condor_slot)
                need_condor_resched = True

        if need_condor_resched:
            condor_reschedule()

    def update_jobs(self):
        
        # handle log events and check if any job terminated
        self.jobs_terminated = self._handle_log_events() or self.jobs_terminated
        
        # need to update schedule (?)
        if self.last_resched and self.jobs_terminated and \
        ((self.timestamp - self.last_resched).seconds > SCHED_TIMEOUT):
            self.update_schedule()
            self.jobs_terminated = False
        
        # handle jobs that are ready to execute
        self._handle_ready_jobs()

#!/usr/bin/env python
# coding: utf-8

from math import ceil
from datetime import datetime
from datetime import timedelta

from workflow import Workflow
from machine import Machine
from machine import MachineStatus
from job import Job
from schedule_entry import ScheduleEntry
from schedule_entry import EntryStatus

from condor import condor_slots
from condor import condor_q
from condor import condor_qedit
from condor import condor_job_completed
from condor import condor_reschedule


DEBUG = True
VM_BOOTTIME = timedelta(seconds=20)
VM_COST_PER_SEC = 1

from time import time
class Timer():
    def __init__(self):
        self.t = time()
        
    def tick(self, msg=""):
        if DEBUG:
            _t = time()
            print "%.2f [ %s ]" % ((_t - self.t), msg)
            self.t = _t
TIMER = Timer()

def job_ready_at(job, entries, timestamp):
    # job is ready when parent jobs finish executing
    _t = timestamp
    for l in entries.values():
        _t = max([_t] + [e.end() for e in l if e.job in job.parents])

    return _t

def e_entry(job, machine, machine_entries, ready_at):
    # ordered entries of the machine
    #machine_entries = [e for e in entries if e.machine == machine]
    #machine_entries.sort(key=lambda x: x.end())
    
    # machine is free?
    #if len(machine_entries) == 0:
    #    return ScheduleEntry(job, machine, ready_at, ready_at + job.pduration)
    
    # if is not free
    # get earlist possible entry in the machine
    sched_entry = None
    it = iter(machine_entries) #ordered
    before = next(it)
    while(sched_entry is None): 
        start = max([before.end(), ready_at])
        end = start + job.pduration
        
        # sched before next
        try:
            after = next(it)
            if (end < after.sched_start):
                sched_entry = ScheduleEntry(job, machine, start, end)
            
            before = after
        
        # there's no next        
        except(StopIteration):
            sched_entry = ScheduleEntry(job, machine, start, end)
    
    return sched_entry

def earliest_entry(job, machines, entries, timestamp):
    """
    Get the earlist entry possible for job to execute in the machines
    after the given timestamp.
    :param job: job to be executed
    :param machines: possible machines to host the job
    :paramm timestamp: job will execute after this timestamp
    :return e_entry: earlist entry
    """
    ready_at = job_ready_at(job, entries, timestamp)
    if job.is_pegasus_job():
        e_entries = [e_entry(job, machines[0], entries[machines[0]], ready_at)]
    else:
        e_entries = [e_entry(job, machine, entries[machine], ready_at) for machine in machines[1:]]

    return min(e_entries, key=lambda x: (x.sched_end, -len(entries[x.machine])))

def insert_entry(sorted_entries, new_entry):
    i = 0
    for e in sorted_entries:
        if e.end() > new_entry.end():
            break
        i = i + 1
    sorted_entries.insert(i, new_entry)
    

def get_nmax(workflow, machines, entries, vm_limit, timestamp):
    """
    Get the max number of machines that can be used by a workflow,
    based on the current state of the workflow execution.
    :param workflow: workflow structure
    :param machines: list of allocated machines
    :param entries: state of the execution
    :param timestamp: barrier timestamp
    :return n: max number of machines that can be used
    """
    TIMER.tick('before get_nmax')
    _machines = list(machines)
    _entries = {m:list(l) for m,l in entries.iteritems()}
    
    # insertion policy
    for job in workflow.ranked_jobs:
        
        if len(_machines) < vm_limit:
            new_machine = Machine()
            boot_entry = ScheduleEntry(None, new_machine, timestamp, timestamp+VM_BOOTTIME)
            new_entry = earliest_entry(job, _machines + [new_machine], dict(_entries.items() + [(new_machine, [boot_entry])]), timestamp)
        
            if new_entry.machine == new_machine:
                _machines.append(new_machine)
                _entries[new_machine] = [boot_entry, new_entry]
            else:
                insert_entry(_entries[new_entry.machine], new_entry)
            
    TIMER.tick('after get_nmax')                
    return len(_machines)

def schedule(workflow, machines, entries, timestamp):
    """
    Schedule the workflow along the machines.
    :param workflow: workflow structure
    :param machines: list of allocated machines 
    :param execution: state of the execution
    :param timestamp: barrier timestamp
    """
    # insertion policy
    for job in workflow.ranked_jobs:
        entry = earliest_entry(job, machines, entries, timestamp)
        insert_entry(entries[entry.machine], entry)
        
def sched_cost_pred(machines, entries, timestamp):
    # cost calculation
    
    vm_runtime = 0
    wf_end = None
    if len(machines) > 1:
        wf_end = entries[machines[1]][-1].end()
    for machine in machines[1:]:
        # machine is running until last job
        start = max(timestamp, entries[machine][0].start())
        finish = max(timestamp, entries[machine][-1].end())
        wf_end = max(wf_end, finish)
        vm_runtime = vm_runtime + (finish - start).seconds 

    # manager
    if wf_end != None:
        start = timestamp
        finish = wf_end
        vm_runtime = vm_runtime + (finish - start).seconds
    
    return vm_runtime * VM_COST_PER_SEC, wf_end
    
def sched_cost_n(workflow, machines, entries, n, timestamp):
    """
    Return the cost used by n machines from timestamp untill end of execution.
    """
    # existing machines
    _machines = list(machines)
    _entries = dict(entries)
    
    # new machine + boot
    for _i in range(n-len(_machines)):
        machine = Machine()
        _machines.append(machine)
        boot_job = Job('boot', None)
        boot_job.pduration = VM_BOOTTIME
        boot_entry = ScheduleEntry(boot_job, machine, timestamp, timestamp+VM_BOOTTIME)
        _entries[machine] = [boot_entry]
    
    TIMER.tick("before sched")    
    schedule(workflow, _machines, _entries, timestamp)
    TIMER.tick("after sched")
    
    cost_pred, _wf_end = sched_cost_pred(_machines, _entries, timestamp)
    return _entries, cost_pred

class BudgetException(Exception):
    pass

def number_of_machines(workflow, machines, entries, nmax, timestamp, budget):
    TIMER.tick('before number of machines')
    _entries = {}
    costs = {}
    
    lowerb = 1 # manager
    upperb = nmax # supoem nmax > 0
    found = False

    while not found:
        i = int(ceil((lowerb + upperb) / 2.0))
        _entries[i], costs[i] = sched_cost_n(workflow, machines, entries, i, timestamp)
        if costs[i] < budget: #satisfied
            lowerb = i
        else:
            upperb = i-1
        if lowerb == upperb:
            found = True
    if lowerb == 1:
        #raise BudgetException("Not enough budget.")
        return None, costs[i], i # i is 2
    
    # analizar custos
    # remover maquinas nao utilizadas
    
    TIMER.tick('after number of machines')
    return _entries[lowerb], costs[lowerb], lowerb

def sync_parents(job, entries, timestamp):
    parents_entries = [e for e in entries if e.status == EntryStatus.scheduled and \
                                             e.job in job.parents]
    print "!!!"
    print [e.job.dag_job_id for e in parents_entries]
    print [j.dag_job_id for j in job.parents]
    for e in parents_entries:
        e.status = EntryStatus.completed
        e.real_end = timestamp
        if e.real_start == None:
            e.real_start = timestamp
        print "--Job", e.job.dag_job_id
        sync_parents(e.job, entries, timestamp)

class Provisioner():
    def __init__(self, vm_limit=32):
        self.vm_limit = vm_limit # user input
        self.budget = 0
        self.timestamp = datetime.now()
        
        self.workflow = Workflow()
        self.entries = {}
        
        manager = Machine()
        manager.status = MachineStatus.manager
        manager.condor_slot = 'local'
        self.machines = [manager]
        
        boot_entry = ScheduleEntry(Job('boot', None), manager, self.timestamp, self.timestamp)
        boot_entry.real_start = self.timestamp
        boot_entry.real_end = self.timestamp
        boot_entry.status = EntryStatus.completed
        self.entries[manager] = [boot_entry]
        
        
    def add_workflow(self, workflow_dir, prediction_file, budget):
        TIMER.tick('before add_workflow')
        self.budget = self.budget + int(budget)
        self.workflow.add_workflow(workflow_dir, prediction_file=prediction_file)
        TIMER.tick('after add_workflow')
            
    def update_schedule(self):
        TIMER.tick('before update_schedule')
        self.update_budget_timestamp()
        
        # completed and running entries will not change
        _entries = {m:[e for e in l if e.status != EntryStatus.scheduled] for m,l in self.entries.iteritems()}

        # Max number of vms
        nmax = get_nmax(self.workflow, self.machines, _entries, self.vm_limit, self.timestamp)
        
        # Get the number of machines to be used
        entries, _cost, _n = number_of_machines(self.workflow, self.machines, _entries, nmax, self.timestamp, self.budget)
     
        # Delay boot entries and remove unused machines
        if entries != None:
            _machines = entries.keys()
            for m in _machines:
                # machine has not been allocated yet
                if m.status == MachineStatus.scheduled:
                    # there's more than only a boot entry
                    if len(entries[m]) <= 1:
                        entries.pop(m, None)
                    # TODO there's enough time between entries to turn off the machine 
                    # there's time between the boot and first entry
                    elif entries[m][0].sched_end < entries[m][1].sched_start:
                        entries[m][0].sched_end = entries[m][1].sched_start
                        entries[m][0].sched_start = entries[m][1].sched_start - entries[m][0].job.pduration  
                     
        # Update schedule
        self.entries = entries
        TIMER.tick('after update_schedule')

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
        if self.entries != None:
            for m in [m for m in self.entries.keys() if self.entries[m][0].status == EntryStatus.scheduled
                                                    and self.entries[m][0].sched_start <= self.timestamp]:
                # allocate vm TODO
                print "Allocation", str(m)
                
                # update machine list
                self.machines.append(m)
                # update machine
                m.status = MachineStatus.allocating
                # update entry
                self.entries[m][0].status = EntryStatus.executing
                self.entries[m][0].real_start = self.timestamp
    
    
    def deallocate_vms(self):
        for m in self.machines:
            if m.status == MachineStatus.manager:
                continue
            
            # if there's no more budget or
            # if there's nothing executing or scheduled to the machine
            if self.entries == None or len([e for e in self.entries[m] if e.status != EntryStatus.completed]) == 0:
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
        allocating_machines.sort(key=lambda x: self.entries[x][0].sched_start)
        i = 0
        for s in slots:
            if s not in [m.condor_slot for m in running_machines]:
                if len(allocating_machines[i:]) > 0:
                    # update machine
                    allocated_machine = allocating_machines[i]
                    allocated_machine.status = MachineStatus.running
                    allocated_machine.condor_slot = s
                    
                    # update entry
                    boot_entry = self.entries[allocated_machine][0]
                    boot_entry.real_end = self.timestamp
                    boot_entry.status = EntryStatus.completed
                
                    i += 1
                    print "++Machine", str(allocated_machine)

    def sync_jobs(self):
        # New jobs
        lines = condor_q(1) # idle jobs
        nq = len(lines)
        _entries = {x for v in self.entries.itervalues() for x in v}
        ns = len([e for e in _entries if e.status == EntryStatus.scheduled])
        ne = len([e for e in _entries if e.status == EntryStatus.executing])
        nc = len([e for e in _entries if e.status == EntryStatus.completed])
        print '[Q: %d S: %d E: %d C: %d]' % (nq,ns,ne,nc)

        scheduled_entries = [e for e in _entries if e.status == EntryStatus.scheduled]
                
        for l in lines:
            global_id, wf_id, dag_job_id, _host = l.split(" ")
            
            for e in [e for e in scheduled_entries if e.job.dag_job_id == dag_job_id \
                                                    and e.job.wf_id == wf_id]:
                # if the target machine is ready
                if (e.machine.status == MachineStatus.running or e.machine.status == MachineStatus.manager) and\
                        len([x for x in self.entries[e.machine] if x.status == EntryStatus.executing]) == 0:
                    e.status = EntryStatus.executing
                    e.job.global_id = global_id
                    e.real_start = self.timestamp
                    if not e.job.is_pegasus_job():
                        condor_qedit(global_id, wf_id, dag_job_id, e.machine.condor_slot)
                    
                    print "++Job", dag_job_id
                    
                    sync_parents(e.job, scheduled_entries, self.timestamp)
                    
        # Completed jobs
        for e in [e for e in _entries if e.status == EntryStatus.executing]:
            if condor_job_completed(e.job.global_id, e.job.wf_id, e.job.dag_job_id):
                e.status = EntryStatus.completed
                if e.real_start == None:
                    e.real_start = self.timestamp
                e.real_end = self.timestamp
                print "--Job", e.job.dag_job_id
        
        condor_reschedule()

#!/usr/bin/env python
# coding: utf-8

from math import ceil

from schedule_entry import ScheduleEntry, EntryStatus, LogKey
from machine import Machine, MachineStatus
from job import Job
from sets import Set

from common import VM_COST_PER_SEC, VM_BOOTTIME

from time import time

DEBUG = False
class Timer():
    def __init__(self):
        self.t = time()
        
    def tick(self, msg=""):
        if DEBUG:
            _t = time()
            print "%.2f [ %s ]" % ((_t - self.t), msg)
            self.t = _t
TIMER = Timer()

def job_ready_at(job, schedule, timestamp):
    # job is ready when parent jobs finish executing
    _t = timestamp
    for e in schedule.entries:
        if e.job in job.parents:
            _t = max([_t, e.end()])

    return _t

def e_entry(job, machine, machine_entries_list, ready_at):
    # machine is free?
    if len(machine_entries_list) == 0:
        return ScheduleEntry(job, machine, ready_at, ready_at + job.pduration)
    
    # if is not free
    # get earlist possible entry in the machine
    sched_entry = None
    it = iter(machine_entries_list) #ordered
    before = next(it)
    while(sched_entry is None): 
        start = max([before.end(), ready_at])
        end = start + job.pduration
        
        # sched before next
        try:
            after = next(it)
            if (end < after.start()):
                sched_entry = ScheduleEntry(job, machine, start, end)
            
            before = after
        
        # there's no next        
        except(StopIteration):
            sched_entry = ScheduleEntry(job, machine, start, end)
    
    return sched_entry

def earliest_entry(job, machines, schedule, timestamp):
    """
    Get the earlist entry possible for job to execute in the machines
    after the given timestamp.
    :param job: job to be executed
    :param machines: possible machines to host the job
    :paramm timestamp: job will execute after this timestamp
    :return e_entry: earlist entry
    """
    ready_at = job_ready_at(job, schedule, timestamp)
    if job.is_pegasus_job():
        return e_entry(job, machines[0], schedule.entries_host[machines[0]], ready_at)
    else:
        print job.dag_job_id
        e_entries = [e_entry(job, machine, schedule.entries_host[machine], ready_at) for machine in machines[1:]]
        for e in e_entries:
            print '  ', e.host.id, e.start(), e.end()
        return min(e_entries, key=lambda x: (x.end(), -len(schedule.entries_host[x.host])))
    

def get_nmax(workflow, machines, schedule, vm_limit, timestamp, local):
    """
    Get the max number of machines that can be used by a workflow,
    based on the current state of the workflow execution.
    :param workflow: workflow structure
    :param machines: list of allocated machines
    :param entries: state of the execution
    :param timestamp: barrier timestamp
    :return n: max number of machines that can be used
    """
    if local:
        vm_boottime = 1
    else:
        vm_boottime = VM_BOOTTIME
    
    TIMER.tick('before get_nmax')
    _machines = list(machines)
    _schedule = Schedule(schedule)
    
    # insertion policy
    need_new_host = True
    done_jobs = [e.job for e in schedule.entries if e.job != None]
    for job in workflow.ranked_jobs:
        if job in done_jobs:
            continue
        
        if need_new_host:
            # last machine added was used, so we need to add a new one
            # if we there's still room to add it else we stop
            if len(_machines) < vm_limit:
                print "ADD HOST"
                
                new_machine = Machine()
                boot_entry = ScheduleEntry(None, new_machine, timestamp, timestamp+vm_boottime)
                _schedule.add_entry_host(boot_entry, new_machine)
                _machines.append(new_machine)
            else:
                break
        
        # schedule with the new machine
        new_entry = earliest_entry(job, _machines, _schedule, timestamp)
        _schedule.add_entry_host(new_entry, new_entry.host)
        for key in _schedule.entries_host.keys():
            print key, len(_schedule.entries_host[key])
            for e in _schedule.entries_host[key]:
                print e.job.dag_job_id if e.job != None else None, e.start(), e.end()
        #raw_input("\nPress Enter to continue...")
        # machine was used?
        if new_entry.host == new_machine:
            need_new_host = True
        else:
            need_new_host = False

    # last machine added wasn't used
    if need_new_host == False:
        print "DISCART LAST HOST"
        _machines = _machines[:-1]
        
    TIMER.tick('after get_nmax')                
    return len(_machines)

def sched_wf(workflow, machines, schedule, timestamp):
    """
    Schedule the workflow along the machines.
    :param workflow: workflow structure
    :param machines: list of allocated machines 
    :param execution: state of the execution
    :param timestamp: barrier timestamp
    """
    done_jobs = [e.job for e in schedule.entries if e.job != None]
    # insertion policy
    for job in workflow.ranked_jobs:
        if job not in done_jobs:
            entry = earliest_entry(job, machines, schedule, timestamp)
            schedule.add_entry_host(entry, entry.host)
        
def sched_cost_pred(machines, schedule, timestamp):
    # cost calculation
    
    vm_runtime = 0
    # manager
    wf_end = max(timestamp, schedule.entries_host[machines[0]][-1].end())
    vm_runtime = vm_runtime + (wf_end - timestamp).seconds
        
    for machine in machines[1:]:
        # machine is running until last job scheduled in the machine
        start = max(timestamp, schedule.entries_host[machine][0].start())
        finish = max(timestamp, schedule.entries_host[machine][-1].end())
        wf_end = max(wf_end, finish)
        vm_runtime = vm_runtime + (finish - start).seconds 
    
    if vm_runtime == 0:
        wf_end = None

    return vm_runtime * VM_COST_PER_SEC, wf_end
    
def sched_cost_n(workflow, machines, schedule, n, timestamp, local):
    """
    Return the cost used by n machines from timestamp untill end of execution.
    """
    if local:
        vm_boottime = 1
    else:
        vm_boottime = VM_BOOTTIME
    
    # existing machines
    _schedule = Schedule(schedule)
    
    if len(machines) < n:
        _machines = list(machines)
        # new machine + boot
        for _i in range(n-len(_machines)):
            machine = Machine()
            _machines.append(machine)
            boot_job = Job('boot', None)
            boot_job.pduration = vm_boottime
            boot_entry = ScheduleEntry(boot_job, machine, timestamp, timestamp+vm_boottime)
            _schedule.add_entry_host(boot_entry, machine)
    else:
        # don't use spare machines
        _machines = sorted(machines[1:], key=lambda m: schedule.entries_host[m][-1].end(), reverse=True)
        for _i in range(len(_machines)-n):
            _machines.pop()
        _machines.insert(0, machines[0]) # manager
    
    TIMER.tick("before sched")    
    sched_wf(workflow, _machines, _schedule, timestamp)
    TIMER.tick("after sched")
    
    _schedule.fix_machines()
    cost_pred, _wf_end = sched_cost_pred(_machines, _schedule, timestamp)
    return _schedule, cost_pred

class BudgetException(Exception):
    pass

def sched_number_of_machines(workflow, machines, schedule, nmax, timestamp, budget, local):
    TIMER.tick('before number of machines')
    _schedules = {}
    costs = {}
    
    lowerb = 1 # manager
    upperb = nmax # supoem nmax > 0
    found = False

    while not found:
        i = int(ceil((lowerb + upperb) / 2.0))
        _schedules[i], costs[i] = sched_cost_n(workflow, machines, schedule, i, timestamp, local)
        if costs[i] < budget: #satisfied
            lowerb = i
        else:
            upperb = i-1
        if lowerb == upperb:
            found = True
    #if lowerb == 1:
        #raise BudgetException("Not enough budget, min cost is: " + str(costs[i]))
        #return _schedules[i], costs[i], i # i is 2
    
    TIMER.tick('after number of machines')
    return _schedules[lowerb], costs[lowerb], lowerb

def insert_entry(sorted_entries, new_entry):
    i = 0
    for e in sorted_entries:
        if e.end() > new_entry.end():
            break
        i = i + 1
    sorted_entries.insert(i, new_entry)

class Schedule():
    def __init__(self, schedule=None):
        if schedule:
            self.entries = Set(schedule.entries)
            self.entries_cid = dict(schedule.entries_cid)
            self.entries_host = {h:list(l) for (h,l) in schedule.entries_host.items()}
        else:
            self.entries = Set()
            self.entries_cid = {}
            self.entries_host = {}
    
    def add_entry_host(self, entry, host):
        if host not in self.entries_host:
            self.entries_host[host] = []
        insert_entry(self.entries_host[host], entry)
        self.entries.add(entry)
    
    def add_entry_cid(self, entry):
        if entry.condor_id == None:
            raise Exception('Entry missing condor id')
        self.entries_cid[entry.condor_id] = entry
        self.entries.add(entry)
    
    def rm_scheduled_entries(self):
        self.entries = Set([e for e in self.entries if e.status != EntryStatus.scheduled])
        self.entries_cid = {cid:e for (cid,e) in self.entries_cid.items() if e.status != EntryStatus.scheduled}
        items = self.entries_host.items()
        self.entries_host = {}
        for machine, entries in items:
            entries = [e for e in entries if e.status != EntryStatus.scheduled]
            if len(entries) > 0:
                self.entries_host[machine] = entries

    def fix_machines(self):
        # Delay boot entries and remove unused machines
        _machines = self.entries_host.keys()
        for m in _machines:
            # machine has not been allocated yet
            if m.status == MachineStatus.scheduled:
                # remove if there's only a boot entry
                if len(self.entries_host[m]) <= 1:
                    self.entries_host.pop(m, None)
                    
                # TODO there's enough time between entries to turn off the machine 
                
                # there's time between the boot and first entry
                elif self.entries_host[m][0].end() < self.entries_host[m][1].start():
                    self.entries_host[m][0].log[LogKey.sched_end] = self.entries_host[m][1].start()
                    self.entries_host[m][0].log[LogKey.sched_start] = self.entries_host[m][1].start() - self.entries_host[m][0].job.pduration

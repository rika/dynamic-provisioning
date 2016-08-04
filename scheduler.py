#!/usr/bin/env python
# coding: utf-8
import sys
import os
import glob
import operator
from xml.etree import ElementTree as ET
from job import Job
from math import ceil
#import htcondor


class Workflow():
    def __init__(self, dir, predfile):

        daxfile = find_dax(dir)
        if not daxfile:
            raise Exception('DAX not found')

        self.jobs = parse_dax(daxfile) ## MUDAR: como dar merge em workflows?
        #parse_predictions(predfile, self.jobs)
    

class Scheduler():
    def __init__(self):
        self.machine_limit = None
        self.total_budget = 0
        self.machines = {}
        
    def schedule(self, dir, predfile, budget):
        ## MUDAR: como tratar o budget para novos workflows
        self.total_budget += budget
        
        workflow = Workflow(dir, predfile)
        
        # Ranking
        self.sorted_jobs = rank_sort(workflow.jobs.values())
        #print([str(job) for job in self.sorted_jobs])
        
        # Max number of machines
        if self.machine_limit:
            self.nmax = self.machine_limit
        else:
            self.nmax = get_nmax(self.sorted_jobs)
        print('nmax '+str(self.nmax))

        # Increase / decrease number of machines
        try:
            n, cost = self.number_of_machines()
        except Exception as e:
            # maybe the budge is not enough
            print(e)
        
        # manage machines
        if n > len(self.machines):
            for i in range(n-len(self.machines)):
                # melhorar nome (API deve devolver o nome)
                name = str(len(self.machines) + i)
                self.machines[name] = Machine(name)
        elif n < len(self.machines):
            # descobrir qual maquina deve ser liberada
            pass

        entries, cost = _schedule(self.sorted_jobs, self.machines.values())
        print('cost '+str(cost))
                
        #sync
        for entry in entries:
            entry.job.sched_entry = entry
            entry.machine.sched_entries.append(entry)
            entry.machine.sched_entries.sort(key=operator.attrgetter('end'))
        
        # print
        for machine in machines.values():
            print(machine)
            for sched in machine.sched_entries:
                print(str(sched))
    
    def number_of_machines(self):
        costs = {}
        
        satisfied, costs[self.nmax] = self.satisfy(self.nmax)
        if satisfied:
            return self.nmax, costs
        
        if self.nmax == 1:
            raise Exception("Not enough budget.")
        
        lowerb = 0
        upperb = self.nmax - 1
        found = False

        while not found:
            i = ceil((lowerb + upperb) / 2.0)
            satisfied, costs[i] = self.satisfy(i)
            if satisfied:
                lowerb = i
            else:
                upperb = i-1
            if lowerb == upperb:
                found = True
        print('n '+str(lowerb) + ' cost ' + str(costs))
        if lowerb == 0:
            raise Exception("Not enough budget.")
        
        return lowerb, costs
        
    def satisfy(self, n):
        now = 0
        boot_time = 20
        
        # existing machines
        machines = list(self.machines)
        
        # new machine + boot
        for i in range(n-len(machines)):
            m = Machine(i)
            m.sched_entries.append(ScheduleEntry(None, m, now, now+boot_time))
            machines.append(m)
            
        _e, cost = _schedule(self.sorted_jobs, machines)
        
        if (cost < self.total_budget):
            return True, cost
        else:
            return False, cost
            
        
def parse_dax(daxfile):
    jobs = {}
    try:
        with open(daxfile) as dax:
            tree = ET.parse(dax)
            root = tree.getroot()
            xmlns = root.tag.replace('adag', '')
            for child in root:
                if child.tag == xmlns + 'job':
                    id = child.attrib['id']
                    jobs[id] = Job(id) # ALTERAR O ID?
            
            for child in root:
                if child.tag == xmlns + 'child':
                    id = child.attrib['ref']
                    for gchild in child:
                        gid = gchild.attrib['ref']
                        jobs[gid].parent_of(jobs[id])
            
    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)

    return jobs
            
class ScheduleEntry():
    def __init__(self, job, machine, start, end):
        self.job = job
        self.machine = machine
        self.start = start
        self.end = end
    
    def __str__(self):
        return "%s @ %s from: %.2f to: %.2f" % (self.job, self.machine, self.start, self.end)

class Machine:    
    def __init__(self, id, local=False):
        self.id = id
        self.sched_entries = []
        self.local=local
    
    def __str__(self):
        return "M" + str(self.id)

# deixar independente, TROCAR self por machine, clonar entries
def earliest_entry(machine, job, now):
    _t = [now]
    for parent in job.parents:
        _t.append(parent.sched_entry.end)

                    
    ready_at = max(_t)
    entries = list(machine.sched_entries)
    
    if len(entries) == 0:
        return ScheduleEntry(job, machine, ready_at, ready_at + job.pduration)
    
    sched_entry = None
    it = iter(entries) #ordered
    before = next(it)
    while(sched_entry is None):
        start = max([before.end, ready_at])
        end = start + job.pduration
        
        try:
            after = next(it)
            if (end < after.start):
                sched_entry = ScheduleEntry(job, machine, start, end)
            
            before = after
                
        except(StopIteration):
            sched_entry = ScheduleEntry(job, machine, start, end)
            
    return sched_entry


def find_dax(path):
    cwd = os.getcwd()
    os.chdir(path)
    files = glob.glob("*.xml") + glob.glob("*.dax")
    os.chdir(cwd)
    if len(files) > 1:
        print('error: more than 1 dax found')
        return None
    
    return os.path.join(path, files[0])
    


def parse_predictions(predfile, jobs):
    
    try:
        with open(predfile) as pred:
            for line in pred:
                id, pduration_str = line.split(' ')
                jobs[id].pduration = float(pduration_str)

    except Exception as e:
        exc_type, exc_obj, exc_tb = sys.exc_info()
        fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
        print(exc_type, fname, exc_tb.tb_lineno)
        print(e)            


def visit(job, visited):
    visited[job] = True
    for child in job.children:
        if visited[child] is False:
            visit(child, visited)
    
    job.rank = job.pduration
    if len(job.children) > 0:
        job.rank += max([child.rank for child in job.children ])
        

def rank_sort(jobs):
    visited = {}
    
    for job in jobs:
        visited[job] = False
        
    for job in jobs:
        if visited[job] is False:
            visit(job, visited)
           
    return sorted(jobs, key=operator.attrgetter('rank'), reverse=True) 

            
def _schedule(sorted_jobs, machines):
    now = 0
    _machines = {}
    entries = []
    for job in sorted_jobs:
        # insertion policy
        e = [earliest_entry(machine, job, now) for machine in machines]
        # earliest finish
        entry = min(e, key=lambda x: x.end)
        entries.append(entry)
        
        if entry.machine in _machines.keys():
            _machines[entry.machine].append(entry)
            _machines[entry.machine].sort(key=operator.attrgetter('end'))
        else:
            _machines[entry.machine] = [entry]
    
    # corrigir
    cost = 1
    for machine in _machines.keys():
        cost +=  machine.sched_entries[-1].end - machine.sched_entries[0].start
    return entries, 0.1*cost 
        
# descobre a largura do grafo
def get_nmax(sorted_jobs):
    machines = [Machine(None)]

    # insertion policy
    now = 0
    for job in sorted_jobs:
        entries = [machine.earliest_entry(job, now) for machine in machines]
        entry = min(entries, key=lambda x: x.end)
        
        new_machine = Machine(None)
        new_entry = new_machine.earliest_entry(job, now)
        
        if new_entry.start < entry.start:
            machines.append(new_machine)
            new_entry.job.sched_entry = new_entry
            new_entry.machine.sched_entries.append(new_entry)
            new_entry.machine.sched_entries.sort(key=operator.attrgetter('end'))    
        else:
            entry.job.sched_entry = entry
            entry.machine.sched_entries.append(entry)
            entry.machine.sched_entries.sort(key=operator.attrgetter('end'))
    
    return len(machines)

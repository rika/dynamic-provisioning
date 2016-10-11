#!/usr/bin/env python
# coding: utf-8

import sys
import os

class Scheduler():
    def __init__(self, workflow):
        
        self.workflow = workflow
        # Ranking
        self.sorted_jobs = rank_sort(workflow.jobs.values())
        #print([str(job) for job in sorted_jobs])
        
    
    def schedule(self, sorted_jobs, machines):
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

    def get_nmax(self, sorted_jobs):
        machines = [Machine(None)]
        entries = []
        
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
    
    
    def earliest_entry(entries, machine, job, timestamp):
        _t = [timestamp]
        parents_entries = [e for e in entries if e.job in job.parents]
        for e in parents_entries:
            _t.append(e.end)
                        
        ready_at = max(_t)
        machine_entries = [e for e in entries if e.machine == machine]
        
        if len(machine_entries) == 0:
            return ScheduleEntry(job, machine, ready_at, ready_at + job.pduration)
        
        sched_entry = None
        machine_entries.sort(key=lambda x: x.end)
        it = iter(machine_entries) #ordered
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


            
    def __number_of_machines(self):
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
        
    def __satisfy(self, n):
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
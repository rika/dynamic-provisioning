#!/usr/bin/env python
# coding: utf-8

import sys
import os

from machine import Machine

class Scheduler():
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

    def get_nmax(self, workflow, machines, execution, timestamp):
        """
        Get the max number of machines that can be used by a workflow,
        based on the current state of the workflow execution.
        :param workflow: workflow structure
        :param machines: list of allocated machines
        :param execution: state of the execution
        :param timestamp: barrier timestamp
        :return n: max number of machines that can be used
        """
        machines = [Machine(None)]
        entries = []
        
        # insertion policy
        now = 0
        for job in workflow.ranked_jobs:
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

    def __schedule(self, workflow, machines, entries, timestamp, vm_cost):
        """
        Schedule the workflow along the machines.
        :param workflow: workflow structure
        :param machines: list of allocated machines 
        :param execution: state of the execution
        :param timestamp: barrier timestamp
        :param vm_cost: cost of a vm per second
        :return entries, cost: the schedule and it's cost 
        """
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
#!/usr/bin/env python
# coding: utf-8

import sys
import os

import operator
from math import ceil
from datetime import datetime
from datetime import timedelta

from scheduler import Scheduler
from workflow import Workflow
from slot import Slot


class Provisioner():
    def __init__(self, vm_limit=None):
        self.vm_limit = vm_limit # user input
        self.total_budget = 0
        self.machines = []
        self.sched_entries = {}
        
    def plan_provision(self, workflow_dir, prediction_file, budget):
        ## MUDAR: como tratar o budget para novos workflows
        self.total_budget += budget
        
        workflow = Workflow(workflow_dir, prediction_file)
        scheduler = Scheduler(workflow)
        
        # Max number of vms
        nmax = min(self.vm_limit, scheduler.get_nmax())
        print('nmax '+str(nmax))

        # Get the number of machines to be used
        try:
            n, cost = self.__number_of_machines()
        except Exception as e:
            # maybe the budge is not enough
            print(e)
        
        # add fake machines 
        if n > len(self.machines):
            for i in range(n-len(self.machines)):
                self.machines[name] = Machine(name, fake=True)
        # set flag to free machines
        elif n < len(self.machines):
            # descobrir qual maquina deve ser liberada
            # faz sentido?
            pass
        
        entries, cost = scheduler.schedule(self.machines.values())
        print('cost '+str(cost))
                
        #sync
        #for entry in entries:
        #    entry.job.sched_entry = entry
        #    entry.machine.sched_entries.append(entry)
        #    entry.machine.sched_entries.sort(key=operator.attrgetter('end'))
        
        # print
        for machine in machines.values():
            print(machine)
            for sched in machine.sched_entries:
                print(str(sched))
    

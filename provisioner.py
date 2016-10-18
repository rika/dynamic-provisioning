#!/usr/bin/env python
# coding: utf-8

import sys
import os


from math import ceil
from datetime import datetime
from datetime import timedelta

from scheduler import Scheduler
from workflow import Workflow
from machine import Machine


class Provisioner():
    def __init__(self, vm_limit=None):
        self.vm_limit = vm_limit # user input
        self.vm_cost = 1
        self.total_budget = 0
        self.machines = []
        self.timestamp = None
        
        self.workflow = Workflow()
        self.scheduler = Scheduler()
        self.execution = Execution()
     
    def add_workflow(self, workflow_dir, prediction_file, budget):
        self.total_budget += budget
        self.workflow.add(workflow_dir, prediction_file)
            
    def plan_provision(self):
        # Max number of vms
        nmax = min(self.scheduler.get_nmax(), self.vm_limit)
        print('nmax '+str(nmax))

        # Get the number of machines to be used
        try:
            n, cost = self.__number_of_machines()
        except Exception as e:
            # maybe the budge is not enough
            print(e)

        # print
        for machine in machines.values():
            print(machine)
            for sched in machine.sched_entries:
                print(str(sched))
    
    def update_tasks(self):
        pass
    
    def update_budget(self, timestamp):
        timestamp
        if self.timestamp != None:
            # Supondo vm_cost em cost/second
            # Supondo que não houve mudança no número de máquinas
            # desde o ultimo self.timestamp
            delta = (self.timestamp - timestamp).seconds
            charged = delta * len(self.machines) * self.vm_cost
            self.total_budget = self.total_budget - charged
        self.timestamp = timestamp
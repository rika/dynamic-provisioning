#!/usr/bin/env python
# coding: utf-8

import unittest
import threading
import os
import sys
import time
import traceback
import uuid
from sets import Set
from datetime import datetime
from time import mktime

from workflow import Workflow
from provisioner import Provisioner
from provisioner import BudgetException
from provisioner import get_nmax
from provisioner import sched_cost_n
from provisioner import number_of_machines

def print_sched(entries):
    k = min([e.start() for e in entries])
    for machine in Set([e.machine for e in entries]):
        print [((e.start()-k).total_seconds(), (e.end()-k).total_seconds()) for e in entries if e.machine == machine]

class Test(unittest.TestCase):

    def setUp(self):
        self.tutorial_dir_1 = '/home/ricardo/git/exemplos/tutorial/work/ricardo/pegasus/diamond/run0001'
        self.tutorial_dir_2 = '/home/ricardo/git/exemplos/tutorial/work/ricardo/pegasus/diamond/run0002'
    
    
    def test_workflow(self):
        print 'test_workflow'
        result = False
        
        w = Workflow()
        w.add_workflow(self.tutorial_dir_1, None)
        for j in w.jobs:
            print j.id
        
        print [j.rank for j in w.ranked_jobs]
        
        result = True 
        self.assertTrue(result)
        
    def test_merge_workflows(self):
        print 'test_merge_workflows'
        result = False
        
        w = Workflow()
        w.add_workflow(self.tutorial_dir_1, None)
        w.add_workflow(self.tutorial_dir_2, None)
         
        for wid in Set([j.wf_id for j in w.jobs]):
            print wid
            for j in [j for j in w.jobs if j.wf_id == wid]:
                print j.id           
        
        result = True 
        self.assertTrue(result)
        
    def test_nmax(self):
        print 'test_nmax'
        result = False
        wf = Workflow()
        wf.add_workflow(self.tutorial_dir_1, None)
        
        print get_nmax(wf, [], [], datetime(2000,1,1)) 
        result = True
        self.assertTrue(result)

    def test_cost_n(self):
        print 'test_cost_n'
        result = False
        wf = Workflow()
        wf.add_workflow(self.tutorial_dir_1, None)
        
        for i in [1,2,3]:
            entries, cost = sched_cost_n(wf, [], [], i, datetime(2000,1,1))
            print i, cost
            print_sched(entries)
        
        
        result = True
        self.assertTrue(result)

    def test_number_of_machines(self):
        print 'test_number_of_machines'
        result = False
        wf = Workflow()
        wf.add_workflow(self.tutorial_dir_1, None)

        try:
            entries, n, costs = number_of_machines(wf, [], [], 1, datetime(2000,1,1), 1)
            print i, n, costs
            print_sched(entries)
        except BudgetException as e:
            print e
        
        for i in range(1,5):
            try:
                entries, n, costs = number_of_machines(wf, [], [], i, datetime(2000,1,1), 10)
                print i, n, costs
                print_sched(entries)
            except BudgetException as e:
                print e

            
        result = True
        self.assertTrue(result)
        
    def test_provisioner(self):
        print 'test_provisoner'
        result = False
        
        prov = Provisioner()
        prov.add_workflow(self.tutorial_dir_1, None, 10)
        
        prov.update_schedule()
        print_sched(prov.entries)
        
        prov.update_schedule()
        print_sched(prov.entries)
        
        
        result = True
        self.assertTrue(result)
        
        



if __name__ == '__main__':
    unittest.main()

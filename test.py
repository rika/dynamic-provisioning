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

from workflow import Workflow
from scheduler import Scheduler
from execution import Execution
from provisioner import Provisioner



class Test(unittest.TestCase):

    def setUp(self):
        self.tutorial_dir_1 = '/home/ricardo/git/exemplos/tutorial/work/ricardo/pegasus/diamond/run0001'
        self.tutorial_dir_2 = '/home/ricardo/git/exemplos/tutorial/work/ricardo/pegasus/diamond/run0002'
    
    
    def test_workflow(self):
        result = False
        
        w = Workflow()
        w.add_workflow(self.tutorial_dir_1, None)
        for j in w.jobs:
            print j.id
        
        print [j.rank for j in w.ranked_jobs]
        
        result = True 
        self.assertTrue(result)
        
    def test_merge_workflows(self):
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
        result = False
        wf = Workflow()
        wf.add_workflow(self.tutorial_dir_1, None)
        
        exc = Execution()
        
        sched = Scheduler()
        sched.get_nmax(wf, [], exc, datetime(2000,1,1))
        result = True
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()

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

from workflow import Workflow
from scheduler import Scheduler
from provisioner import Provisioner


class Test(unittest.TestCase):

    def setUp(self):
        self.tutorial_dir_1 = '/home/ricardo/git/exemplos/tutorial/work/ricardo/pegasus/diamond/run0001'
        self.tutorial_dir_2 = '/home/ricardo/git/exemplos/tutorial/work/ricardo/pegasus/diamond/run0002'
    
    
    def test_workflow(self):
        result = False
        
        w = Workflow(self.tutorial_dir_1, None)
        for j in w.jobs:
            print j.id
        
        result = True 
        self.assertTrue(result)
        
    def test_merge_workflows(self):
        result = False
        
        w1 = Workflow(self.tutorial_dir_1, None)
        w2 = Workflow(self.tutorial_dir_2, None)
        w1.merge(w2)
        
         
        for w in Set([j.wf_id for j in w1.jobs]):
            print w
            for j in [j for j in w1.jobs if j.wf_id == w]:
                print j.id           
        
        result = True 
        self.assertTrue(result)
        
    def test_provisioner(self):
        result = False
        prov = Provisioner()
        prov.plan_provision(self.tutorial_dir_1, None, 10)
        
        result = True
        self.assertTrue(result)


if __name__ == '__main__':
    unittest.main()

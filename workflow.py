#!/usr/bin/env python
# coding: utf-8

import sys
import os

import glob
from xml.etree import ElementTree as ET

from job import Job
from slot import Slot
from sched

class Workflow():
    def __init__(self, dir, predfile):

        daxfile = self.__get_dax(dir)
        wf_id = self.__get_wf_id(dir)
        self.jobs = self.__parse_dax(daxfile, wf_id)
        #self.__parse_predictions(predfile, self.jobs)
    
    def merge(self, workflow):
        self.jobs = self.jobs + workflow.jobs
    
    def __get_dax(self, path):
        cwd = os.getcwd()
        os.chdir(path)
        files = glob.glob("*.xml") + glob.glob("*.dax")
        os.chdir(cwd)
        
        return os.path.join(path, files[0])
    
    def __get_wf_id(self, path):
        cwd = os.getcwd()
        os.chdir(path)
        with open('braindump.txt') as f:
            for line in f:
                if 'wf_uuid' in line:
                    wf_id = line.split(' ')[1]
        os.chdir(cwd)
        
        return wf_id
    
    def __parse_dax(self, daxfile, wf_id):
        jobs = {}
        try:
            with open(daxfile) as dax:
                tree = ET.parse(dax)
                root = tree.getroot()
                xmlns = root.tag.replace('adag', '')
                for child in root:
                    if child.tag == xmlns + 'job':
                        id = child.attrib['id']
                        jobs[id] = Job(id, wf_id)
                
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
    
        return jobs.values()
    
    def __parse_predictions(self, predfile, jobs):
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
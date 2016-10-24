#!/usr/bin/env python
# coding: utf-8

import sys
import os

import glob
from xml.etree import ElementTree as ET
from job import Job
from datetime import timedelta


def parse_dax(workflow_dir):
    cwd = os.getcwd()
    os.chdir(workflow_dir)
    
    # get dax file
    files = glob.glob("*.xml") + glob.glob("*.dax")
    daxfile = os.path.join(workflow_dir, files[0])
    
    # get workflow id
    with open('braindump.txt') as f:
        for line in f:
            if 'wf_uuid' in line:
                wf_id = line.split(' ')[1].rstrip('\n')
                break
    os.chdir(cwd)
    
    # parse dax
    jobs = {}
    try:
        with open(daxfile) as dax:
            tree = ET.parse(dax)
            root = tree.getroot()
            xmlns = root.tag.replace('adag', '')
            # get jobs
            for child in root:
                if child.tag == xmlns + 'job':
                    id = child.attrib['id']
                    name = child.attrib['name']
                    jobs[id] = Job(id, name, wf_id)
            
            # get dependencies
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
    
def parse_predictions(predfile, jobs):
    for job in jobs:
        job.pduration = timedelta(seconds=1)
    '''
    with open(predfile) as pred:
        for line in pred:
            id, pduration_str = line.split(' ')
            jobs[id].pduration = float(pduration_str)
    '''

def visit(job, visited):
    visited[job] = True
    for child in job.children:
        if visited[child] is False:
            visit(child, visited)
    
    job.rank = job.pduration
    if len(job.children) > 0:
        job.rank += max([child.rank for child in job.children ])
    
def rank_jobs(jobs):
    # obs: RANK NÃO MUDA durante a  execução, depende somenete da pduration
    # mas pduration não se altera durante a execução
    visited = {}
    
    for job in jobs:
        visited[job] = False
        
    for job in jobs:
        if visited[job] is False:
            visit(job, visited)
           
    return sorted(jobs, key=lambda x: x.rank, reverse=True)
    


class Workflow():
    def __init__(self):
        self.jobs = []
    
    def add_workflow(self, workflow_dir, prediction_file):
        jobs = parse_dax(workflow_dir)
        parse_predictions(prediction_file, jobs)
        
        self.jobs = self.jobs + jobs
        self.ranked_jobs = rank_jobs(self.jobs)
        

 
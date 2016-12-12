#!/usr/bin/env python
# coding: utf-8

import os
import glob
from datetime import timedelta
#from xml.etree import ElementTree as ET

from job import Job
import csv

def parse_dag(workflow_dir):
    cwd = os.getcwd()
    os.chdir(workflow_dir)
    
    # get workflow id
    with open('braindump.txt') as f:
        for line in f:
            if 'wf_uuid' in line:
                wf_id = line.split(' ')[1].rstrip('\n')
                break
        
    # get dax file
    files = glob.glob("*.dag")
    dagfile = os.path.join(workflow_dir, files[0])
    os.chdir(cwd)
    
    # parse dax
    jobs = {}
    with open(dagfile) as dag:
        for line in dag:
            if line.startswith('JOB'):
                dag_job_id = line.split(' ')[1]
                jobs[dag_job_id] = Job(dag_job_id, wf_id)
            elif line.startswith('PARENT'):
                values = line.split(' ')
                child = values[4].rstrip('\n')
                parent = values[2]
                jobs[child].parents.append(jobs[parent])
                jobs[parent].children.append(jobs[child])

    return wf_id, jobs.values()
'''
def parse_dax(workflow_dir):
    cwd = os.getcwd()
    os.chdir(workflow_dir)
    
    # get workflow id
    with open('braindump.txt') as f:
        for line in f:
            if 'wf_uuid' in line:
                wf_id = line.split(' ')[1].rstrip('\n')
                break
    
    # get dax file
    files = glob.glob("*.xml") + glob.glob("*.dax")
    daxfile = os.path.join(workflow_dir, files[0])
    os.chdir(cwd)
    
    # parse dax
    jobs = {}
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
            
    
    return jobs.values()
'''    
def parse_predictions(predfile, jobs):
    with open(predfile) as pred:
        reader = csv.reader(pred, delimiter=',')
        avg_exec = {}
        for row in reader:
            try:
                duration = round(float(row[1]))
                avg_exec[row[0]] = timedelta(seconds=duration)
            except ValueError:
                pass
            
        for job in jobs:
            for jt in avg_exec.keys():
                if jt in job.dag_job_id:
                    job.pduration = avg_exec[jt]
                    break

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
    
    def add_workflow(self, workflow_dir, prediction_file=None):
        wf_id, jobs = parse_dag(workflow_dir)
        if prediction_file:
            parse_predictions(prediction_file, jobs)
        
        self.jobs = self.jobs + jobs
        if prediction_file:
            self.ranked_jobs = rank_jobs(self.jobs)
        return wf_id

    def has_jobs_to_sched(self, schedule):
        done_jobs = [e.job for e in schedule.entries if e.job != None]
        not_done = len([j for j in self.jobs if j not in done_jobs])
        print not_done, '/', len(done_jobs)
        return not_done > 0
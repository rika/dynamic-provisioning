#!/usr/bin/env python
# coding: utf-8

from subprocess import Popen
from subprocess import PIPE
import re

''' JobStatus
0    Unexpanded     U
1    Idle           I
2    Running        R
3    Removed        X
4    Completed      C
5    Held           H
6    Submission_err E

'''

def condor_slots():
    p = re.compile('\d+.\d+.\d+.\d+')
    cmd = "condor_status -format '%s ' Name -format '%s;' MyAddress"
    proc = Popen(cmd, stdout=PIPE, shell=True)
    return[(row.split()[0], p.search(row).group()) for row in filter(None, proc.communicate()[0].split(';'))] 

def condor_q(condor_id):
    cmd = "condor_q -constraint 'ClusterId == "+str(condor_id)+"' -format '%s ' pegasus_wf_uuid -format '%s ' pegasus_wf_dag_job_id -format '%s' RemoteHost" 
    proc = Popen(cmd, stdout=PIPE, shell=True)
    result = proc.communicate()[0].split()
    if len(result) == 2:
        result.append('')
    return result

def condor_history(condor_id):
    cmd = "condor_history -constraint 'ClusterId == "+str(condor_id)+"' -format '%s ' pegasus_wf_uuid -format '%s ' pegasus_wf_dag_job_id -format '%s' LastRemoteHost" 
    proc = Popen(cmd, stdout=PIPE, shell=True)
    result = proc.communicate()[0].split()
    if len(result) == 2:
        result.append('')
    return result

def condor_idle():
    cmd = "condor_q -constraint 'JobStatus == 1' -format '%s ' ClusterId -format '%s ' pegasus_wf_uuid -format '%s\n' pegasus_wf_dag_job_id"
    proc = Popen(cmd, stdout=PIPE, shell=True)
    return filter(None, proc.communicate()[0].split("\n"))

def condor_qedit(condor_id, wf_id, dag_job_id, target_machine):
    cmd = "condor_qedit -constraint 'JobStatus == 1 &&" + \
        " ClusterId == " + condor_id + " &&" + \
        " pegasus_wf_uuid == \"" + wf_id + "\" &&" + \
        " pegasus_wf_dag_job_id == \""+ dag_job_id + "\"'" + \
        " Requirements '( ( Target.Name== \""+ target_machine +"\" ) )'"
    proc = Popen(cmd, stdout=PIPE, shell=True)
    if proc.communicate()[0] != 'Set attribute "Requirements".\n':
        raise Exception("condor_qedit failed")

def condor_reschedule():
    cmd = "condor_reschedule"
    proc = Popen(cmd, stdout=PIPE, shell=True)
    proc.communicate()
    
def condor_rm_jobs():
    cmd = "condor_rm -all"
    proc = Popen(cmd, stdout=PIPE, shell=True)
    proc.communicate()

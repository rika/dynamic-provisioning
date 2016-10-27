from subprocess import Popen
from subprocess import PIPE

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
    cmd = 'condor_status -autoformat Name'
    proc = Popen(cmd, stdout=PIPE, shell=True)
    results = proc.communicate()[0].split("\n")
    return filter(None, results)

def condor_q(JobStatus):
    cmd = "condor_q -constraint 'JobStatus == "+str(JobStatus)+"' -format '%s ' GlobalJobId -format '%s ' pegasus_wf_uuid -format '%s ' pegasus_wf_dag_job_id -format '%s\n' RemoteHost |awk '{ gsub(/\"/, \"\"); print $1\" \"$2\" \"$3\" \"$4;  }'"
    proc = Popen(cmd, stdout=PIPE, shell=True)
    return filter(None, proc.communicate()[0].split("\n"))

def condor_qedit(global_id, wf_id, dag_job_id, target_machine):
    cmd = "condor_qedit -constraint 'JobStatus == 1 &&" + \
        " GlobalJobId == \"" + global_id + "\" &&" + \
        " pegasus_wf_uuid == \"" + wf_id + "\" &&" + \
        " pegasus_wf_dag_job_id == \""+ dag_job_id + "\"'" + \
        " Requirements '( ( Target.Name== \""+ target_machine +"\" ) )'"
    proc = Popen(cmd, stdout=PIPE, shell=True)
    if proc.communicate()[0] != 'Set attribute "Requirements".\n':
        raise Exception("condor_qedit failed")
    

def condor_job_completed(global_id, wf_id, job_id):
    cmd = "condor_history -constraint 'JobStatus == 4 &&" + \
        " GlobalJobId == \"" + global_id + "\" &&" + \
        " pegasus_wf_uuid == \"" + wf_id + "\" &&" + \
        " pegasus_wf_dag_job_id == \""+ job_id + "\"'" + \
        " -autoformat GlobalJobId | grep " + global_id
    proc = Popen(cmd, stdout=PIPE, shell=True)
    results = filter(None, proc.communicate()[0].split("\n"))
    if len(results) == 0:
        return False
    else:
        return True
    
def condor_reschedule():
    cmd = "condor_reschedule"
    proc = Popen(cmd, stdout=PIPE, shell=True)
    proc.communicate()
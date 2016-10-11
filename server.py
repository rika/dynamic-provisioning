#!/usr/bin/env python
# coding: utf-8
import os
import sys
import socket
from threading import Thread
from _socket import timeout
from common import PORT
from scheduler import Scheduler
from scheduler import Machine

from subprocess import Popen, PIPE

    
def receive(client_socket):    
    # Receive messages from client and concatenate them
    chunks = []
    while True:
        try:
            data = client_socket.recv(1024)
            if not data:
                break
            else:
                chunks.append(data.decode('UTF-8'))
            
        except Exception as e:
            exc_type, exc_obj, exc_tb = sys.exc_info()
            fname = os.path.split(exc_tb.tb_frame.f_code.co_filename)[1]
            print(exc_type, fname, exc_tb.tb_lineno)
            print(e)   
            break
    
    return ''.join(chunks)

def main(local=False):
    scheduler = Scheduler()
    
    # Socket setup    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', PORT))
    server_socket.listen(1)
    server_socket.settimeout(5)
    
    # Condor Connection
    
    # query slots
    cmd = 'condor_status -l |grep ^Name'
    proc = Popen(cmd, stdout=PIPE, shell=True)
    results = proc.communicate()[0].replace("Name = ","").split("\n")
    results = filter(None, results)
    for r in results:
        scheduler.machines[r] = Machine(r, local=True)
    
    print results
    
    # Wait for connections until receive a stop message
    stop = False
    while(not stop):
        ## Update time
        
        try:
            client_socket, _addr = server_socket.accept()
            msg = receive(client_socket)

            # Stop server
            if '--stop' in msg:
                stop = True
                
            # TODO: combine
            else:
                # Schedule a new Workflow instance
                (dir, pred, budget) = msg.split(' ')
                sched = scheduler.schedule(dir, pred, float(budget))
                
                ## Update execution
                ## Update expected completion time
                
            client_socket.close()
        except timeout:
            
            # Events:  
            # - slots changed?
            # - end of job:
            #   - need resched?
            #   - disallocate machine?
            # - scheduled task:
            #   - create new machine
            
            # Query Slots
            cmd = 'condor_status -l |grep ^Name'
            proc = Popen(cmd, stdout=PIPE, shell=True)
            results = proc.communicate()[0].replace("Name = ","").split("\n")
            results = filter(None, results)
            for r in results:
                scheduler.machines[r] = Machine(r, local=True)
            
            print(len(results))
            print(results)
            if len(results) > 0:
                pass
                #machine = results[0]["Name"]
                # Query Jobs
                ''' JobStatus
                0    Unexpanded     U
                1    Idle           I
                2    Running        R
                3    Removed        X
                4    Completed      C
                5    Held           H
                6    Submission_err E
                '''
                '''
                results = schedd.query("JobStatus =?= 1", ["GlobalJobId", "pegasus_wf_dag_job_id", "pegasus_wf_uuid"] , limit=1000)
                print(len(results))
                print(results)
                
                for r in results:
                    print('editing: ' + r["GlobalJobId"])
                    schedd.edit('GlobalJobId == "'+r["GlobalJobId"]+'"','Requirements','(Name == "'+machine+'")')
                '''
                # get status
                # compare
                # action
                
                # monitor machines boots
                #if task completed
                    #if not ok:
                        #reschedule
    print("stop message received")
    
if __name__ == '__main__':
    if len(sys.argv) > 1 and (sys.argv[1] == '--local' or sys.argv[1] == '-l'):
        main(local=True)
    else:
        main()
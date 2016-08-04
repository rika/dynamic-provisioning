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
from common import CONDOR_CONFIG
from common import CONDOR_LIB

os.environ["CONDOR_CONFIG"] = CONDOR_CONFIG
sys.path.append(CONDOR_LIB)
import htcondor
        
    
def receive(client_socket):    
    # Receive messages from 1 client and concatenate them
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
    coll = htcondor.Collector(socket.getfqdn())
    sched_ad = coll.locate(htcondor.DaemonTypes.Schedd)
    schedd = htcondor.Schedd(sched_ad)
    
    # Get existing slots
    if local:
        results = coll.query(htcondor.AdTypes.Startd, "true", ["Name", "SlotID"])
        scheduler.limit_machine = len(results)
        for r in results:
            scheduler.machines[r["Name"]] = Machine(r["Name"], local=True)
    
    # Wait for connections until receive a stop message
    stop = False
    while(not stop):
        try:
            client_socket, _addr = server_socket.accept()
            msg = receive(client_socket)

            # Stop server
            if '--stop' in msg:
                stop = True
                
            # Schedule a new Workflow instance
            else:
                (dir, pred, budget) = msg.split(' ')
                sched = scheduler.schedule(dir, pred, float(budget))
                

            client_socket.close()
        except timeout:
            # Query Slots
            results = coll.query(htcondor.AdTypes.Startd, "true", ["Name", "SlotID"])
            print(len(results))
            print(results)
            
            machine = results[0]["Name"]
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
            results = schedd.query("JobStatus =?= 1", ["GlobalJobId", "pegasus_wf_dag_job_id", "pegasus_wf_uuid"] , limit=1000)
            print(len(results))
            print(results)
            
            for r in results:
                print('editing: ' + r["GlobalJobId"])
                schedd.edit('GlobalJobId == "'+r["GlobalJobId"]+'"','Requirements','(Name == "'+machine+'")')
            # get status
            # compare
            # action
            
            # monitor machines boots
            #if task completed
                #if not ok:
                    #reschedule
            pass
    print("stop message received")
    
if __name__ == '__main__':
    if len(sys.argv) > 1 and (sys.argv[1] == '--local' or sys.argv[1] == '-l'):
        main(local=True)
    else:
        main()
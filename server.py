#!/usr/bin/env python
# coding: utf-8
import os
import sys
import socket
from threading import Thread
from _socket import timeout
from common import PORT

from provisioner import Provisioner
from provisioner import sched_cost_pred
from statistics import Statistics

TIMEOUT = 10

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
    provisioner = Provisioner(vm_limit=3)
    statistics = Statistics()
    
    # Socket setup    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', PORT))
    server_socket.listen(1)
    server_socket.settimeout(TIMEOUT)
    
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
                
            else:
                provisioner.update_budget_timestamp()
                
                # Schedule a new Workflow instance
                (dir, pred, budget) = msg.split(' ')
                provisioner.add_workflow(dir, pred, budget)
                provisioner.update_schedule()

            client_socket.close()
        except timeout:
            
            # Events:  
            # - slots changed?
            # - end of job:
            #   - need resched?
            #   - disallocate machine?
            # - scheduled task:
            #   - create new machine
            
            provisioner.update_budget_timestamp()
            
            # Update and sync vms
            provisioner.allocate_new_vms()
            provisioner.deallocate_vms()
            provisioner.sync_machines()
            
            # Update and sync jobs
            provisioner.sync_jobs()
            
            # Resched?
            
            # Statistics
            cost_pred, wf_end = sched_cost_pred(provisioner.machines, provisioner.entries, provisioner.timestamp)
            statistics.schedshot(provisioner.timestamp, provisioner.budget, cost_pred, wf_end)
            statistics.snapshot(provisioner.timestamp, provisioner)
        
    statistics.jobs(provisioner)
    statistics.dump()
    print("stop message received")
    
if __name__ == '__main__':
    if len(sys.argv) > 1 and (sys.argv[1] == '--local' or sys.argv[1] == '-l'):
        main(local=True)
    else:
        main()
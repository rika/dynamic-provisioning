#!/usr/bin/env python
# coding: utf-8
import sys
import socket
from _socket import timeout
from common import PORT

from monitor import Monitor
from provisioner import Provisioner
from provisioner import sched_cost_pred
from statistics import Statistics

TIMEOUT = 5

def receive(client_socket):    
    # Receive messages from client and concatenate them
    chunks = []
    while True:
        data = client_socket.recv(1024)
        if not data:
            break
        else:
            chunks.append(data.decode('UTF-8'))

    
    return ''.join(chunks)



def main(local=False):
    provisioner = Provisioner(vm_limit=3)
    monitor = None
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
            msgs = msg.split(' ')

            # Stop server
            if '--stop' in msgs[0]:
                stop = True
            elif len(msgs) == 1:
                if monitor != None:
                    raise Exception("Only one workflow can be monitored at a time")
                
                # Parse workflow to monitor
                monitor = Monitor()
                wf_dir = msgs[0]
                monitor.add_workflow(wf_dir)
                
            else:
                if provisioner.monitor == True:
                    raise Exception("Only one workflow can be monitored at a time")
                
                provisioner.update_budget_timestamp()
                
                # Parse and schedule a new Workflow instance
                wf_dir = msgs[0]
                pred = msgs[1]
                budget = msgs[2] 
                provisioner.add_workflow(wf_dir, prediction_file=pred, budget=budget)
                provisioner.update_schedule()

            client_socket.close()
        except timeout:
            
            if monitor == None:
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
            else:
                monitor.update_timestamp()
                monitor.sync_machines()
                monitor.sync_jobs()
    
    if monitor == None:
        entries = {x for v in provisioner.entries.itervalues() for x in v}
        statistics.jobs(entries)
    else:
        statistics.jobs(monitor.entries)
    statistics.dump()
    print("stop message received")
    
if __name__ == '__main__':
    if len(sys.argv) > 1 and (sys.argv[1] == '--local' or sys.argv[1] == '-l'):
        main(local=True)
    else:
        main()
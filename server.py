#!/usr/bin/env python
# coding: utf-8

import socket
from _socket import timeout
from common import PORT

from monitor import Monitor
from provisioner import Provisioner
from statistics import Statistics
import argparse
import sys
from azure_config import AzureConfig

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



def main(vm_limit, config_path, skip_setup, local):
    azure_config = None
    if config_path:
        azure_config = AzureConfig(config_path)
       
    provisioner = Provisioner(vm_limit+1, azure_config, skip_setup, local) #+manager
    monitor = None
    statistics = Statistics()
    
    # Socket setup    
    server_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    server_socket.bind(('localhost', PORT))
    server_socket.listen(1)
    server_socket.settimeout(TIMEOUT)
    
    # Wait for connections until receive a stop message
    done = False
    while(True):
        if done and ((monitor != None and monitor.logwatcher.watching_none()) \
                or (monitor == None and provisioner.logwatcher.watching_none())):
            break
                
        try:
            client_socket, _addr = server_socket.accept()
            msg = receive(client_socket)
            msgs = msg.split(' ')

            # Stop server
            if '--stop' in msgs[0]:
                done = True
            elif len(msgs) == 1:
                if monitor != None:
                    raise Exception("Only one workflow can be monitored at a time")
                
                # Parse workflow to monitor
                monitor = Monitor()
                wf_dir = msgs[0]
                monitor.add_workflow(wf_dir)
                
            else:
                if monitor != None:
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
            
            if monitor == None and provisioner.workflow.jobs:
                provisioner.update_budget_timestamp()
                
                # Update and sync vms
                provisioner.allocate_new_vms()
                provisioner.deallocate_vms()
                provisioner.sync_machines()
                
                # Update, sync jobs, may reschedule
                provisioner.update_jobs()
                
                # Statistics
                provisioner.update_wf_pred()
                statistics.schedshot(provisioner)
                statistics.snapshot(provisioner.timestamp, provisioner.schedule.entries, provisioner.machines)
            elif monitor and monitor.workflow.jobs:
                monitor.update_timestamp()
                monitor.sync_machines()
                monitor.sync_jobs()
                statistics.snapshot(monitor.timestamp, monitor.entries, monitor.machines)
                
        sys.stdout.flush()
        
    if monitor == None:
        entries = provisioner.schedule.entries
    else:
        entries = monitor.entries
    
    statistics.jobs(entries)
    statistics.dump()
    print("stop message received")
    
if __name__ == '__main__':
    parser = argparse.ArgumentParser(description='Provisioner server')
    parser.add_argument('-n', '--vm_limit', type=int, default=32)
    parser.add_argument('-l', '--local', action='store_true', default=False)
    parser.add_argument('-c', '--config_path', help='azure config path')
    parser.add_argument('-s', '--skip_setup', action='store_true', default=True)
    args = parser.parse_args()
    main(args.vm_limit, args.config_path, args.skip_setup, args.local)
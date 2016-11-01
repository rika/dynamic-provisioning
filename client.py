#!/usr/bin/env python
# coding: utf-8

import os
import socket
import argparse
from common import PORT
from time import sleep
from subprocess import Popen
from subprocess import PIPE


def run_workflow(path):
    cmd = 'pegasus-run ' + path
    proc = Popen(cmd, stdout=PIPE, shell=True)
    proc.communicate()

def workflow_finish(path):
    cmd = 'tail -n1 ' + path
    proc = Popen(cmd, stdout=PIPE, shell=True)
    results = proc.communicate()[0]
    if "MONITORD_FINISHED" in results:
        return True
    else:
        return False

def send(msg):
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', PORT))
    client_socket.send(msg.encode('UTF-8'))
    client_socket.close()
    
def main():
    parser = argparse.ArgumentParser(description='Manage the provisioner')
    
    parser.add_argument('-r', '--run', action='store_true', help='run the workflow')
    parser.add_argument('-w', '--wait', action='store_true', help='wait for the workflow to finish and send stop message')
    parser.add_argument('-s', '--stop', action='store_true', help='stop the scheduler')
    parser.add_argument('-d', '--dir', help='directory of the workflow to be scheduled')
    parser.add_argument('-p', '--pred', help='prediction file of the workflow to be scheduled')
    parser.add_argument('-b', '--budget', help='budget of the workflow to be scheduled')
    args = parser.parse_args()

    # Stop message
    if args.stop is True:
        msg = '--stop'
    else:
        # Directory + Prediction + Budget message
        if args.dir is None:
            raise Exception('missing option --dir')
        if args.pred is None or args.budget is None:
            msg = args.dir
        else:
            if not os.path.isdir(args.dir):
                raise Exception('error: invalid workflow directory')
            if not os.path.isfile(args.pred):
                raise Exception('error: invalid prediction file')
            try:
                "${:,.2f}".format(float(args.budget))
            except ValueError:
                raise Exception('error: invalid budget')
        
            # Ok
            msg = args.dir +' '+ args.pred +' '+ args.budget

    # Send
    send(msg)
    
    if args.run == True:
        run_workflow(args.dir)
    
    if args.wait == True:
        path = os.path.join(args.dir, 'jobstate.log')
        sleep(10)
        while not workflow_finish(path):
            sleep(10)
        send('--stop')
        
if __name__ == '__main__':
    main()
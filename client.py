#!/usr/bin/env python
# coding: utf-8
import sys
import os
import socket
import argparse
from subprocess import call
from common import PORT


def main():
    parser = argparse.ArgumentParser(description='Manage the scheduler')
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
        if args.pred is None:
            raise Exception('missing option --pred')
        if args.budget is None:
            raise Exception('missing option --budget')
    
        if not os.path.isdir(args.dir):
            raise Exception('error: invalid directory')
    
        if not os.path.isfile(args.pred):
            raise Exception('error: invalid prediction file')
        try:
            "${:,.2f}".format(float(args.budget))
        except ValueError:
            raise Exception('error: invalid budget')
    
        # Ok
        msg = args.dir +' '+ args.pred +' '+ args.budget

    # Send
    client_socket = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
    client_socket.connect(('localhost', PORT))
    client_socket.send(msg.encode('UTF-8'))
    client_socket.close()

    # run workflow
    #call(['pegasus-run', args.dir], shell=False)
        
if __name__ == '__main__':
    main()
#!/usr/bin/env python
# coding: utf-8

import uuid

class Machine():
    def __init__(self, machine_id=None):
        if machine_id == None:
            self.id = str(uuid.uuid4().get_hex())
        else:
            self.id = machine_id
        self.status = MachineStatus.scheduled
        self.condor_slot = "new slot"
    
    def allocate(self):
        print 'allocating', self.id
        self.status = MachineStatus.allocating

    def deallocate(self):
        print 'deallocating', self.id
        self.status = MachineStatus.deallocating
    
class MachineStatus():
    manager = 0
    scheduled = 1
    allocating = 2
    running = 3
    deallocating = 4
    
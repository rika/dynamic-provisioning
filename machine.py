#!/usr/bin/env python
# coding: utf-8

import uuid

class Machine():
    def __init__(self, id=None):
        if id == None:
            id = str(uuid.uuid4().get_hex())
        self.id = id
        self.status = MachineStatus.scheduled
        self.condor_slot = "new slot"
        
    
    def __str__(self):
        return "S" + str(self.id)
    
class MachineStatus():
    manager = 0
    scheduled = 1
    allocating = 2
    running = 3
    deallocating = 4
    
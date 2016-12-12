#!/usr/bin/env python
# coding: utf-8

import uuid
import threading
import os 


def _allocate(exp, name, master_addr, user):
    exp.provision(tags=[name], has_public_ip=False)
    exp.wait(tags=[name])
    #setup
    dir_path = os.path.dirname(os.path.realpath(__file__))
    exp.put([name], os.path.join(dir_path,'worker.sh'), '/tmp/worker.sh', user=user, priv=True)
    exp.run([name], 'sudo chmod 777 /tmp/worker.sh && sudo /tmp/worker.sh '+master_addr, user=user, priv=True)


class Machine():
    def __init__(self, machine_id=None):
        if machine_id == None:
            self.id = str(uuid.uuid4().get_hex())
        else:
            self.id = machine_id
        self.status = MachineStatus.scheduled
        self.condor_slot = "new slot"
    
    def allocate(self, exp, master_addr, user):
        print 'allocating', self.id
        if exp:
            self.azure_allocate_thread = threading.Thread(
                target=_allocate,
                args=(
                    exp,
                    self.id,
                    master_addr,
                    user,
                ),
            )
            self.azure_allocate_thread.start()
        self.status = MachineStatus.allocating

    def deallocate(self, exp):
        print 'deallocating', self.id
        if exp:
            self.azure_deallocate_thread = threading.Thread(
                target=exp.deprovision,
                args=(
                    [self.id],
                ),
            )
            self.azure_deallocate_thread.start()
        self.status = MachineStatus.deallocating
    
class MachineStatus():
    manager = 0
    scheduled = 1
    allocating = 2
    running = 3
    deallocating = 4
    
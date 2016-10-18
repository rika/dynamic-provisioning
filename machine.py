#!/usr/bin/env python
# coding: utf-8

import uuid

class Machine:
    def __init__(self, id=None, is_fake=False, is_local=False):
        if id == None:
            id = str(uuid.uuid4().get_hex())
        self.id = id
        self.is_fake=is_fake
        self.is_local=is_local
        
    
    def __str__(self):
        return "S" + str(self.id)
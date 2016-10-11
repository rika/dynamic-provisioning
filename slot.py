#!/usr/bin/env python
# coding: utf-8

import uuid

class Slot:
    def __init__(self, id=None, local=False, fake=False):
        if id == None:
            id = str(uuid.uuid4().get_hex())
        self.id = id
        self.local=local
        self.fake=fake
        
    
    def __str__(self):
        return "S" + str(self.id)
#!/bin/bash

MASTER_ADDR=$1

## CONDOR ##
service condor stop

cat >/etc/condor/condor_config.local <<EOF

CONDOR_HOST = $MASTER_ADDR

FULL_HOSTNAME = $(hostname -f)
UID_DOMAIN = $(hostname -d)
FILESYSTEM_DOMAIN = $(hostname -d)
TRUST_UID_DOMAIN = True

CONDOR_IDS = $(id -u condor).$(id -g condor)

DAEMON_LIST = MASTER, STARTD

# security
ALLOW_WRITE = 10.*, *.$(hostname -d)
ALLOW_READ = \$(ALLOW_WRITE)

# default policy
START = True
SUSPEND = False
CONTINUE = True
PREEMPT = False
KILL = False

EOF

service condor start

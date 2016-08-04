
PORT = 50001

CONDOR_CONFIG = '/home/ricardo/condor-8.3.4-x86_64_Ubuntu14-unstripped/etc/condor_config'
CONDOR_LIB = '/home/ricardo/condor-8.3.4-x86_64_Ubuntu14-unstripped/lib/python'

if __name__ == '__main__':
    import os
    import sys
    import socket
    os.environ["CONDOR_CONFIG"] = CONDOR_CONFIG
    sys.path.append(CONDOR_LIB)
    import htcondor
    coll = htcondor.Collector(socket.getfqdn())
    sched_ad = coll.locate(htcondor.DaemonTypes.Schedd)
    schedd = htcondor.Schedd(sched_ad)
    
    ## Test area

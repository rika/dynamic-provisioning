

class Execution():
    def __init__(self):
        self.entries = []

    def earliest_entry(entries, machine, job, timestamp):
        _t = [timestamp]
        parents_entries = [e for e in entries if e.job in job.parents]
        for e in parents_entries:
            _t.append(e.end)
                        
        ready_at = max(_t)
        machine_entries = [e for e in entries if e.machine == machine]
        
        if len(machine_entries) == 0:
            return ScheduleEntry(job, machine, ready_at, ready_at + job.pduration)
        
        sched_entry = None
        machine_entries.sort(key=lambda x: x.end)
        it = iter(machine_entries) #ordered
        before = next(it)
        while(sched_entry is None):
            start = max([before.end, ready_at])
            end = start + job.pduration
            
            try:
                after = next(it)
                if (end < after.start):
                    sched_entry = ScheduleEntry(job, machine, start, end)
                
                before = after
                    
            except(StopIteration):
                sched_entry = ScheduleEntry(job, machine, start, end)
                
        return sched_entry
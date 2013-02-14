
"""
Heterogeneous Earliest Finish Time -- A static scheduling heuristic

      Performance-effective and low-complexity task scheduling
                    for heterogeneous computing
                                by
             Topcuoglu, Haluk; Hariri, Salim Wu, M
     IEEE Transactions on Parallel and Distributed Systems 2002


Cast of Characters:

job - the job to be allocated
agentstate - dict {agent: [jobs-run-on-agent-in-order]}
jobstate - dict {job: agent-on-which-job-is-run}
prec - dict {job: (jobs which directly precede job)}
prec - dict {job: (jobs which directly succeed job)}
compcost - function :: job, agent -> time to compute job on agent
commcost - function :: job, job, agent, agent -> time to transfer results
                       of one job needed by another between two agents

[1]. http://en.wikipedia.org/wiki/Heterogeneous_Earliest_Finish_Time
"""

from functools import partial
from collections import namedtuple
from util import reverse_dict

Event = namedtuple('Event', 'job start end')

def wbar(ni, agents, compcost):
    """ Average computation cost """
    return sum(compcost(ni, agent) for agent in agents) / len(agents)

def cbar(ni, nj, agents, commcost):
    """ Average communication cost """
    n = len(agents)
    npairs = n * (n-1)
    return 1. * sum(commcost(ni, nj, a1, a2) for a1 in agents for a2 in agents
                                        if a1 != a2) / npairs

def ranku(ni, agents, succ,  compcost, commcost):
    """ Rank of job """
    rank = partial(ranku, compcost=compcost, commcost=commcost,
                           succ=succ, agents=agents)
    w = partial(wbar, compcost=compcost, agents=agents)
    c = partial(cbar, agents=agents, commcost=commcost)

    if ni in succ:
        return w(ni) + max(c(ni, nj) + rank(nj) for nj in succ[ni])
    else:
        return w(ni)

def ready_to_send(job, events):
    for e in events:
        if e.job == job:
            return e.end

def start_time(job, agentstate, jobstate, prec, commcost, agent):
    """ Earliest time that job can be executed on agent """
    agent_ready = agentstate[agent][-1].end if agentstate[agent] else 0
    if job in prec:
        comm_ready = max(ready_to_send(p, agentstate[jobstate[p]])
                   + commcost(p, job, agent, jobstate[p]) for p in prec[job])
    else:
        comm_ready = 0
    return max(agent_ready, comm_ready)

def allocate(job, agentstate, jobstate, prec, compcost, commcost):
    """ Allocate job to the machine with earliest finish time

    Operates in place
    """
    st = partial(start_time, job, agentstate, jobstate, prec, commcost)
    ft = lambda machine: st(machine) + compcost(job, machine)

    agent = min(agentstate.keys(), key=ft)
    start = st(agent)
    end = ft(agent)

    agentstate[agent].append(Event(job, start, end))
    jobstate[job] = agent

def makespan(agentstate):
    """ Finish time of last job """
    return max(v[-1].end for v in agentstate.values() if v)

def schedule(succ, agents, compcost, commcost):
    rank = partial(ranku, agents=agents, succ=succ,
                          compcost=compcost, commcost=commcost)
    prec = reverse_dict(succ)

    jobs = set(succ.keys()) | set(x for xx in succ.values() for x in xx)
    jobs = sorted(jobs, key=rank)

    agentstate = {agent: [] for agent in agents}
    jobstate = dict()
    for job in reversed(jobs):
        allocate(job, agentstate, jobstate, prec, compcost, commcost)

    return agentstate, jobstate

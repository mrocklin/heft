from heft.core import (wbar, cbar, ranku, schedule, Event, earliest_finish_time,
        makespan)
from functools import partial

def compcost(job, agent):
    if agent.islower():
        return job
    if agent.isupper():
        return job * 2

def commcost(ni, nj, A, B):
    if A == B:
        return 0
    if A.islower() == B.islower():
        return 5
    else:
        return 10

dag = {2: (3, 4),
       3: (5,),
       4: (5, 6)}

def test_wbar():
    assert wbar(1, 'abc', compcost) == 1
    assert wbar(1, 'ABC', compcost) == 2

def test_cbar():
    assert cbar(1, 2, 'abc', commcost) == 5
    assert cbar(1, 2, 'Abc', commcost) == (10 + 10 + 5) / 3.

def test_ranku():
    rank = partial(ranku, agents='abc', commcost=commcost, compcost=compcost,
            succ=dag)
    w = partial(wbar, agents='abc', compcost=compcost)
    c = partial(cbar, agents='abc', commcost=commcost)
    assert isinstance(rank(6), (int, float))
    assert rank(6) == w(6)
    assert rank(4) == w(4) + c(4, 6) + rank(6)
    assert rank(2) > rank(4) > rank(3) > rank(5)

def test_earliest_finish_time():
    agentstate = {'a': [Event(2, 0, 3)], 'b': []}
    jobstate = {2: 'a'}
    prec = {3: (2,)}
    assert earliest_finish_time(3, agentstate, jobstate, prec, compcost,
        commcost, 'a') == 6
    assert earliest_finish_time(3, agentstate, jobstate, prec, compcost,
        commcost, 'b') == 3 + 5

def test_schedule():
    agentstate, jobstate = schedule(dag, 'abc', compcost, commcost)
    a = jobstate[2]
    b = jobstate[6]
    c = jobstate[3]
    assert agentstate == {a: [Event(2, 0, 2), Event(4, 2, 6), Event(5, 11, 16)],
                          b: [Event(6, 5, 11)],
                          c: [Event(3, 5, 8)]}

def test_makespan():
    assert makespan({'a': [Event(0, 0, 1), Event(1, 2, 3)],
                     'b': [Event(2, 3, 4)]}) == 4

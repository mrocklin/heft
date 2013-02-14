from heft.core import (wbar, cbar, ranku, schedule, Event, start_time,
        makespan, ready_to_send)
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
        return 3
    else:
        return 6

dag = {3: (5,),
       4: (6,),
       5: (7,),
       6: (7,),
       7: (8, 9)}

def test_wbar():
    assert wbar(1, 'abc', compcost) == 1
    assert wbar(1, 'ABC', compcost) == 2

def test_cbar():
    assert cbar(1, 2, 'abc', commcost) == 3
    assert cbar(1, 2, 'Abc', commcost) == (6 + 6 + 3) / 3.

def test_ranku():
    rank = partial(ranku, agents='abc', commcost=commcost, compcost=compcost,
            succ=dag)
    w = partial(wbar, agents='abc', compcost=compcost)
    c = partial(cbar, agents='abc', commcost=commcost)
    assert isinstance(rank(6), (int, float))
    assert rank(8) == w(8)
    assert rank(7) == w(7) + c(7, 9) + rank(9)
    assert sorted((3,4,5,6,7,8,9), key=rank) == [4, 3, 6, 5, 7, 9, 8][::-1]

def test_earliest_finish_time():
    agentstate = {'a': [Event(2, 0, 3)], 'b': []}
    jobstate = {2: 'a'}
    prec = {3: (2,)}
    assert start_time(3, agentstate, jobstate, prec, commcost, 'a') == 3
    assert start_time(3, agentstate, jobstate, prec, commcost, 'b') == 3 + 3

def test_schedule():
    agentstate, jobstate = schedule(dag, 'abc', compcost, commcost)
    a = jobstate[4]
    b = jobstate[3]
    c = (set('abc') - set((a, b))).pop()
    print a, b, c
    print agentstate
    assert agentstate == {a: [Event(4, 0, 4), Event(6, 4, 10),
                              Event(7, 11, 18), Event(9, 18, 27)],
                          b: [Event(3, 0, 3), Event(5, 3, 8), Event(8, 21, 29)],
                          c: []}

def test_makespan():
    assert makespan({'a': [Event(0, 0, 1), Event(1, 2, 3)],
                     'b': [Event(2, 3, 4)]}) == 4

def test_ready_to_send():
    events = [Event(0, 1, 2), Event(1, 2, 3), Event(2, 3, 4)]
    assert ready_to_send(0, events) == 2
    assert ready_to_send(1, events) == 3

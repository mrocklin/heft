from core import wbar, cbar, ranku
from functools import partial

def compcost(job, agent):
    if agent.islower():
        return job
    if agent.isupper():
        return job * 2

def commcost(ni, nj, A, B):
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

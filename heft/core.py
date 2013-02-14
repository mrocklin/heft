from functools import partial

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

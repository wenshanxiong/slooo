#!/usr/bin/env xonsh
import argparse

from tests.mongodb.test_main import *
from tests.rethink.test_main import *
from tests.tidb.test_main import *
from tests.copilot.test_main import *

def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", type=str, help="mongodb/rethinkdb/tidb/copilot")
    parser.add_argument("--iters", type=int, default=1, help="number of iterations")
    parser.add_argument("--workload", type=str, default="resources/workloads/workloada", help="workload path")
    parser.add_argument("--server-configs", type=str, default="server_configs.json", help="server config path")
    parser.add_argument("--runtime", type=int, default=300, help="runtime")
    parser.add_argument("--exps", type=str, default="noslow", help="experiments to be ran saperated by commas(,)")
    parser.add_argument("--exp-type", type=str, default="follower", help="leader/follower/both")
    parser.add_argument("--ondisk", type=str, default="disk", help="in memory(mem) or on disk (disk)")
    parser.add_argument("--threads", type=int, default=250, help="no. of logical clients")
    parser.add_argument("--output-path", type=str, default="results", help="results output path")
    parser.add_argument("--cleanup", action='store_true', help="clean's up the servers")
    parser.add_argument("--fault-snooze", type=int, default=0, help="After how long from the start of sending reqs should the fault be injected")
    opt = parser.parse_args()
    return opt

def main(opt):
    if opt.cleanup:
        if opt.system == "mongodb":
            DB = MongoDB(opt=opt)
        elif opt.system == "rethinkdb":
            DB = RethinkDB(opt=opt)
        elif opt.system == "tidb":
            DB = TiDB(opt=opt)
        elif opt.system == "copilot":
            DB = Copilot(opt=opt)
        DB.cleanup()
        return

    for iter in range(1,opt.iters+1):
        exps = [exp.strip() for exp in opt.exps.split(",")]
        for exp in exps:
            if opt.system == "mongodb":
                DB = MongoDB(opt=opt,trial=iter,exp=exp)
            elif opt.system == "rethinkdb":
                DB = RethinkDB(opt=opt,trial=iter,exp=exp)
            elif opt.system == "tidb":
                DB = TiDB(opt=opt,trial=iter,exp=exp)
            elif opt.system == "copilot":
                DB = Copilot(opt=opt,trial=iter,exp=exp)
            DB.run()
            sleep 30

if __name__ == "__main__":
    opt = parse_opt()
    main(opt)

#!/usr/bin/env xonsh
import argparse

from rethink.test_main import RethinkDB
from utils.common_utils import config_parser

def parse_opt():
    parser = argparse.ArgumentParser()
    parser.add_argument("--system", type=str, default="rethinkdb", help="mongodb/rethinkdb/tidb/copilot")
    parser.add_argument("--iters", type=int, default=1, help="number of iterations")
    parser.add_argument("--workload", type=str, default="../YCSB/workloads/workloadf", help="workload path")
    parser.add_argument("--server-configs", type=str, default="./rethink/server_configs_local.json", help="server config path")
    parser.add_argument("--runtime", type=int, default=300, help="runtime")
    parser.add_argument("--exps", type=str, default="noslow", help="experiments to be ran saperated by commas(,)")
    parser.add_argument("--exp-type", type=str, default="follower", help="leader/follower/both")
    parser.add_argument("--ondisk", type=str, default="disk", help="in memory(mem) or on disk (disk)")
    parser.add_argument("--threads", type=int, default=250, help="no. of logical clients")
    parser.add_argument("--output-path", type=str, default="results", help="results output path")
    parser.add_argument("--cleanup", action='store_true', help="clean's up the servers")
    parser.add_argument("--fault-snooze", type=int, default=0, help="After how long from the start of sending reqs should the fault be injected")
    parser.add_argument("--fault-configs", type=str, default="./faults/fault_config.json", help="fault injection config path")
    opt = parser.parse_args()
    return opt

def main(opt):
    if opt.cleanup:
        DB = RethinkDB(opt=opt)
        DB.cleanup()
        return

    for iter in range(1,opt.iters+1):
        exps = [exp.strip() for exp in opt.exps.split(",")]

        for exp in exps:

            if exp == "2" or exp == "3" or exp == "4":
                DB = RethinkDB(opt=opt,trial=iter,exp=exp, fault_level=None)
                DB.run()
                sleep 30
            else:
                # get fault level and pointbreak config
                fault_cfg = config_parser(opt.fault_configs)
                pb_cfg = fault_cfg["pointbreak"]
                fault_level = fault_cfg["fault_level"][exp]

                # calculate the fault levels for testing. If point break is not activated, only the user defined level will be tested.
                target, start, end, step = pb_cfg["target"], pb_cfg["start"], pb_cfg["end"], pb_cfg["step"]

                if pb_cfg["activate"]:
                    pointbreak_checkpoints = [checkpoint for checkpoint in range(start, end, -step)]
                    
                    for checkpoint in pointbreak_checkpoints:
                        fault_level[target] = checkpoint
                        DB = RethinkDB(opt=opt,trial=iter,exp=exp, fault_level=fault_level)
                        DB.run()
                        sleep 30
                else:
                    DB = RethinkDB(opt=opt,trial=iter,exp=exp, fault_level=fault_level)
                    DB.run()
                    sleep 30


if __name__ == "__main__":
    opt = parse_opt()
    main(opt)

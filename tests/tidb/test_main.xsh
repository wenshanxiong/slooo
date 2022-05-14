#!/usr/bin/env xonsh

import sys
import json
import yaml
import logging
from multiprocessing import Process

from utils.quorum import Quorum
from utils.common_utils import *
from faults.fault_inject import fault_inject

class TiDB(Quorum):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pd_configs = self.nodes["pd"]
        results_path = os.path.join(self.output_path, "tidb_{}_{}_{}_{}_results".format(self.exp_type,"swapon" if self.swap else "swapoff", self.ondisk, self.threads))
        mkdir -p @(results_path)
        self.results_txt = os.path.join(results_path,"{}_{}.txt".format(self.exp,self.trial))
        self.setup_yaml = os.path.join(os.path.dirname(__file__), "setup.yaml")
        self.setup_updt_yaml = os.path.join(os.path.dirname(__file__), "setup_updt.yaml")
        print(self.setup_yaml, self.setup_updt_yaml)

    def config_yaml(self):
        data = None
        with open(self.setup_yaml, "r") as f:
            data = f.read()
        
        # data = data.replace("tidb", "root")
        data = data.replace("<pd_host>", self.pd_configs["ip"])
        data = data.replace("<pd_deploy_dir>", self.pd_configs["deploy_dir"])
        data = data.replace("<pd_data_dir>", self.pd_configs["data_dir"])

        for idx, cfg in enumerate(self.server_configs):
            data = data.replace(f"<s{idx+1}_host>", cfg["ip"])
            data = data.replace(f"<s{idx+1}_deploy_dir>", cfg["deploy_dir"])
            data = data.replace(f"<s{idx+1}_data_dir>", cfg["data_dir"])
            data = data.replace(f"<s{idx+1}_port>", str(20160 + int(cfg["port_offset"])))
            data = data.replace(f"<s{idx+1}_status_port>", str(20180 + int(cfg["port_offset"])))

        with open(self.setup_updt_yaml, "w") as f:
            f.write(data)
    
    def server_setup(self):
        super().server_setup()
        for cfg in self.server_configs:
            ssh -i ~/.ssh/id_rsa @(cfg["ip"]) @(f"sudo sh -c 'sudo mkdir -p {cfg['datadir']}; sudo chmod o+w {cfg['datadir']}'")

    def start_db(self):
        super().start_db()
        scp @(self.setup_updt_yaml) @(self.pd_configs["ip"]):~/
        ssh -i ~/.ssh/id_rsa @(self.pd_configs["ip"]) @(f"{self.pd_configs['tiup']} cluster deploy mytidb v4.0.0 ./setup_updt.yaml --user tidb -y")

        for cfg in self.server_configs:
            run_tikv = os.path.join(cfg["deploy_dir"], "scripts/run_tikv.sh")
            ssh -i ~/.ssh/id_rsa @(cfg["ip"]) @(f"sudo sed -i 's#bin/tikv-server#taskset -ac {cfg['cpu']} bin/tikv-server#g' {run_tikv}")

        ssh -i ~/.ssh/id_rsa @(self.pd_configs["ip"]) @(f"{self.pd_configs['tiup']} cluster start mytidb")
        sleep 30

    def db_init(self):
        super().db_init()
        tiup ctl:v4.0.0 pd config set label-property reject-leader dc 1 -u @(f"http://{self.pd_configs['ip']}:2379")    # leader is restricted to s3
        sleep 10

        followerip=self.server_configs[0]["ip"]
        pids=$(ssh -i ~/.ssh/id_rsa @(followerip) "sh -c 'pgrep tikv-server'")
        pids = pids.split()
        for pid in pids:
            ac = $(ssh -i ~/.ssh/id_rsa @(followerip) @(f"sh -c 'taskset -pc {pid}'"))
            print(ac, self.server_configs[0])
            if self.server_configs[0]["cpu"] ==  int(ac.split(": ")[1]):
                secondarypid = pid

        if self.exp_type=="follower":
            self.fault_pids=[int(secondarypid)]
            for cfg in self.server_configs:
                if cfg == followerip:
                    self.fault_server_config=cfg

    # benchmark_load is used to run the ycsb load and wait until it completes.
    def benchmark_load(self):
        super().benchmark_load()
        taskset -ac @(self.client_configs['cpus']) @(self.client_configs["ycsb"]) load tikv -P @(self.workload) -p tikv.pd=@(self.pd_configs["ip"]):2379 --threads=@(self.threads)

    # ycsb run exectues the given workload and waits for it to complete
    def benchmark_run(self):
        super().benchmark_run()
        taskset -ac @(self.client_configs['cpus']) @(self.client_configs["ycsb"]) run tikv -P @(self.workload) -p maxexecutiontime=@(self.runtime) -p tikv.pd=@(self.pd_configs["ip"]):2379 --threads=@(self.threads) > @(self.results_txt)

    
    def tidb_cleanup(self):
        ssh -i ~/.ssh/id_rsa @(self.pd_configs["ip"]) @(f"{self.pd_configs['tiup']} cluster destroy mytidb -y")


    def run(self):
        self.config_yaml()
        super().run()

#!/usr/bin/env xonsh

import ast
import json
import logging
import pymongo
from multiprocessing import Process

from utils.quorum import Quorum
from utils.common_utils import *
from faults.fault_inject import fault_inject

class MongoDB(Quorum):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        results_path = os.path.join(self.output_path, "mongodb_{}_{}_{}_{}_results".format(self.exp_type,"swapon" if self.swap else "swapoff", self.ondisk, self.threads))
        mkdir -p @(results_path)
        self.results_txt = os.path.join(results_path,"{}_{}.txt".format(self.exp,self.trial))
        self.init_script_path = os.path.join(os.path.dirname(__file__), "init_script.js")
        self.fetchprimary_path = os.path.join(os.path.dirname(__file__), "fetchprimary.js")
        self.cleanup_script_path = os.path.join(os.path.dirname(__file__), "cleanup_script.js")


    # init is called to initialise the db servers
    def server_setup(self):
        super().server_setup()
        for cfg in self.server_configs:
            ssh -i ~/.ssh/id_rsa @(cfg["ip"]) @(f"sudo sh -c 'sudo mkdir -p {cfg['dbpath']}; sudo chmod o+w {cfg['dbpath']}'")


    # start_db starts the database instances on each of the server
    def start_db(self):
        super().start_db()
        for cfg in self.server_configs:
            ssh -i ~/.ssh/id_rsa @(cfg["ip"]) @(f"sh -c 'numactl --interleave=all taskset -ac {cfg['cpu']} {cfg['mongod']} --replSet rs0 --bind_ip localhost,{cfg['name']} --port {cfg['port']} --fork --logpath {cfg['logpath']} --dbpath {cfg['dbpath']}'")


    # db_init initialises the database
    def db_init(self):
        super().db_init()
        client_mongo = self.client_configs["mongo"] 
        @(self.client_configs["mongo"]) --host @(self.server_configs[0]["host"]) < @(self.init_script_path)
        
        # Wait for startup
        sleep 30

        response = $(@(client_mongo) --host @(self.server_configs[0]["host"]) < @(self.fetchprimary_path) | tail -n +5 | head -n -1)
        response = response[response.find("["):]
        response = response.replace('\n', '')
        response = response.replace('\t', '')
        mongo_servers = json.loads("{\"json\": " + response + "}")["json"]

        for mongo_server in mongo_servers:
            if mongo_server["stateStr"] == "PRIMARY":
                primary_server = mongo_server["name"]
            elif mongo_server["stateStr"] == "SECONDARY":
                secondary_server = mongo_server["name"]

        for cfg in self.server_configs:
            if primary_server == cfg["host"]:
                self.primaryip = cfg["ip"]
                self.primaryhost = cfg["host"]
                primary_server_config = cfg
            elif secondary_server == cfg["host"]:
                self.secondaryip = cfg["ip"]
                secondary_server_config = cfg


        #####PID LOGIC STILL TO BE ADDED
        pids=$(ssh -i ~/.ssh/id_rsa @(self.primaryip) "sh -c 'pgrep mongo'")
        pids = pids.split()
        for pid in pids:
            ac = $(ssh -i ~/.ssh/id_rsa @(self.primaryip) @(f"sh -c 'taskset -pc {pid}'"))
            if primary_server_config["cpu"] ==  int(ac.split(": ")[1]):
                primarypid = pid

        pids=$(ssh -i ~/.ssh/id_rsa @(self.secondaryip) "sh -c 'pgrep mongo'")
        pids = pids.split()
        for pid in pids:
            ac = $(ssh -i ~/.ssh/id_rsa @(self.secondaryip) @(f"sh -c 'taskset -pc {pid}'"))
            if secondary_server_config["cpu"] ==  int(ac.split(": ")[1]):
                secondarypid = pid

        if self.exp_type == "follower":
            self.fault_pids=[int(secondarypid)]
            self.fault_server_config=secondary_server_config
        elif self.exp_type == "leader":
            self.fault_pids=[int(primarypid)]
            self.fault_server_config=primary_server_config

        # Disable chaining allowed
        @(client_mongo) --host @(self.primaryhost) --eval "cfg = rs.config();\
                                                         cfg.settings.chainingAllowed = false;\
                                                         rs.reconfig(cfg);"
        # for cfg in self.server_configs:
        #     if cfg["name"] == primary_server:
        #         continue
        #     # @(client_mongo) --host @(cfg["host"]) --eval @("db.adminCommand( { replSetSyncFrom: '{}' })".format(self.primaryhost))
        #     conn = pymongo.MongoClient(cfg["host"])
        #     res = conn.admin.command('replSetSyncFrom', self.primaryhost)

        # Set WriteConcern==majority    in order to make it consistent between all DBs
        @(client_mongo) --host @(self.primaryhost) --eval "cfg = rs.config();\
                                                         cfg.settings.getLastErrorDefaults = { j:true, w:'majority', wtimeout:10000 };\
                                                         rs.reconfig(cfg);"


    # benchmark_load is used to run the ycsb load and wait until it completes.
    def benchmark_load(self):
        super().benchmark_load()
        taskset -ac @(self.client_configs['cpus']) @(self.client_configs["ycsb"]) load mongodb -s -P @(self.workload) -threads @(self.threads) -p mongodb.url=@(f"mongodb://{self.primaryhost}/ycsb?w=majority&readConcernLevel=majority")


    # ycsb run exectues the given workload and waits for it to complete
    def benchmark_run(self):
        super().benchmark_run()
        taskset -ac @(self.client_configs['cpus']) @(self.client_configs["ycsb"]) run mongodb -s -P @(self.workload) -threads @(self.threads)  -p maxexecutiontime=@(self.runtime) -p mongodb.url=@(f"mongodb://{self.primaryhost}/ycsb?w=majority&readConcernLevel=majority") > @(self.results_txt)


    # cleanup is called at the end of the given trial of an experiment
    def db_cleanup(self):
        super().db_cleanup()
        client_mongo = self.client_configs["mongo"]
        @(client_mongo) --host @(self.primaryhost) < @(self.cleanup_script_path)
        @(client_mongo) --host @(self.primaryhost) --eval "db.getCollectionNames().forEach(function(n){db[n].remove()});"


    def init_script(self):
        rm -rf @(self.init_script_path)
        members = ""
        for idx, cfg in enumerate(self.server_configs):
            server_host = cfg["host"]
            members = members + "{{ _id: {}, host: \"{}\" }},".format(idx, server_host)

        query = "rs.initiate( {{_id : \"rs0\", members: [{}]}})".format(members[:-1])
        with open(self.init_script_path,"w") as f:
            f.write(query)


    def run(self):
        self.init_script()
        super().run()
        
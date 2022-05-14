#!/usr/bin/env xonsh

import pdb
import sys
import json
import logging
from rethinkdb import r
from multiprocessing import Process

from utils.quorum import Quorum
from utils.common_utils import *
from faults.fault_inject import fault_inject

class RethinkDB(Quorum):
    def __init__(self, **kwargs):
        super().__init__(**kwargs)
        self.pyserver = self.server_configs[len(self.server_configs)-1]["ip"]
        self.pyserver_port = 28015 + int(self.server_configs[len(self.server_configs)-1]["port_offset"])
        results_path = os.path.join(self.output_path, "rethink_{}_{}_{}_{}_results".format(self.exp_type,"swapon" if self.swap else "swapoff", self.ondisk, self.threads))
        mkdir -p @(results_path)
        self.results_txt = os.path.join(results_path,"{}_{}_{}.txt".format(self.exp,self.trial,self.fault_level))

    # server_setup prepares the data folder for each rethinkdb server
    def server_setup(self):
        super().server_setup()
        for cfg in self.server_configs:
            ssh -i ~/.ssh/id_rsa @(cfg["ip"]) @(f"sudo sh -c 'sudo mkdir -p {cfg['dbpath']}; sudo chmod o+w {cfg['dbpath']}'")

    # start_db starts the database instances on each of the server
    def start_db(self):
        super().start_db()
        cluster_port = None
        join_ip = None
        for idx, cfg in enumerate(self.server_configs):
            if idx==0:
                # print('rethinkdb --directory {} --port-offset {} --bind all --server-name {} --daemon'.format(cfg['dbpath'], cfg['port_offset'], cfg['name']))
                ssh -i ~/.ssh/id_rsa @(cfg["ip"]) @(f"sh -c 'taskset -ac {cfg['cpu']} rethinkdb --directory {cfg['dbpath']} --port-offset {cfg['port_offset']} --bind all --server-name {cfg['name']} --daemon'")
                join_ip = cfg["ip"]
                cluster_port = 29015 + int(cfg["port_offset"])
            else:
                # print('rethinkdb --directory {} --port-offset {} --join {}:{} --bind all --server-name {} --daemon'.format(cfg['dbpath'], cfg['port_offset'], join_ip, cluster_port, cfg['name']))
                ssh -i ~/.ssh/id_rsa @(cfg["ip"]) @(f"sh -c 'taskset -ac {cfg['cpu']} rethinkdb --directory {cfg['dbpath']} --port-offset {cfg['port_offset']} --join {join_ip}:{cluster_port} --bind all --server-name {cfg['name']} --daemon'")

    # db_init initialises the database and table
    def db_init(self):
        super().db_init()
        print("connecting to server ", self.pyserver)
        conn = r.connect(self.pyserver, self.pyserver_port)

        # Connection established
        try:
            r.db('workload').table_drop('usertable').run(conn)
        except Exception as e:
            print("Could not delete table")
        try:
            r.db_drop('workload').run(conn)
        except Exception as e:
            print("Could not delete db")

        try:
            r.db_create('workload').run(conn)
            r.db('workload').table_create('usertable', replicas=len(self.server_configs), primary_key='__pk__').run(conn)
        except Exception as e:
            print("Could not create table")

        # Print table status
        table_status = list(r.db('rethinkdb').table('table_status').run(conn))
        print(json.dumps(table_status, indent=2, sort_keys=True))

        # get leader's (pid, ip) and follower's (pid, ip)
        leader = table_status[0]['raft_leader']
        for replica in table_status[0]['shards'][0]['replicas']:
            if replica['server'] != leader:
                follower = replica['server']
                break

        res = list(r.db('rethinkdb').table('server_status').run(conn))
        name_PID_IP = {}
        for n in res:
            name_PID_IP[n['name']] = n['process']['pid'],n['network']['canonical_addresses'][0]['host']
            self.server_pids.append(n['process']['pid'])

        leader_pid, leader_ip = name_PID_IP[leader]
        follower_pid, follower_ip = name_PID_IP[follower]
        print(leader, leader_ip, leader_pid)
        print(follower, follower_ip, follower_pid)

        self.primaryip = leader_ip

        if self.exp_type == "follower":
            fault_replica=follower
            self.fault_pids=str(follower_pid)
            connect=leader
            self.pyserver = leader_ip
        elif self.exp_type == "leader":
            fault_replica=leader
            self.fault_pids=str(leader_pid)
            connect=follower
            self.pyserver = follower_ip

        for cfg in self.server_configs:
            if cfg["name"] == fault_replica:
                self.fault_server_config = cfg

        for cfg in self.server_configs:
            if cfg["name"] == leader:
                self.primaryport = 28015 + int(cfg["port_offset"])
            if cfg["name"] == connect:
                self.pyserver_port = 28015 + int(cfg["port_offset"])

        

    # db_cleanup cleans up the database and table
    def db_cleanup(self):
        super().db_cleanup()
        print("connecting to server ", self.pyserver)
        try:
            conn = r.connect(self.pyserver, self.pyserver_port)
        except Exception as e:
            print("Could not connect to server")
        # Connection established
        try:
            r.db('workload').table_drop('usertable').run(conn)
        except Exception as e:
            print("Could not delete table")
        try:
            r.db_drop('workload').run(conn)
        except Exception as e:
            print("Could not delete db")

        print("DB and table deleted")

    # ycsb run exectues the given workload and waits for it to complete
    def benchmark_run(self):
        super().benchmark_run()
        print("Running the workload...")
        print(self.pyserver, self.pyserver_port)
        taskset -ac @(self.client_configs['cpus']) python3 @(self.client_configs['workload']) @(self.pyserver) @(self.pyserver_port) > @(self.results_txt)

    # test_run is the main driver function
    def run(self):
        super().run()

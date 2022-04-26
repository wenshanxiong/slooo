from rethinkdb import r
from statistics import mean, median
import json
import os
import time
import matplotlib.pyplot as plt

pyserver = 'localhost'
pyserver_port = 28017
replicas_cnt = 3

median_latency = [] # records median lantency of each second's operations
throughput = [] # records throughput (unit: tx / sec)


def start_server():
    os.system("sudo rethinkdb --directory /data1/rethinkdb_data1 --port-offset 0 --bind all --server-name rethink-1 --daemon")
    os.system("sudo rethinkdb --directory /data2/rethinkdb_data2 --port-offset 1 --join localhost:29015 --bind all --server-name rethink-2 --daemon")
    os.system("sudo rethinkdb --directory /data3/rethinkdb_data3 --port-offset 2 --join localhost:29015 --bind all --server-name rethink-3 --daemon")


def db_init():
    global conn

    print("connecting to server ", pyserver)
    conn = r.connect(pyserver, pyserver_port)

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
        r.db('workload').table_create('usertable', replicas=replicas_cnt, primary_key='__pk__').run(conn)
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

    leader_pid, leader_ip = name_PID_IP[leader]
    follower_pid, follower_ip = name_PID_IP[follower]
    print(leader, leader_ip, leader_pid)
    print(follower, follower_ip, follower_pid)


def execute_write_read_queries():
    global median_latency, throughput
    conn = r.connect(pyserver, pyserver_port)

    # transaction queries
    write_query = r.db('workload').table("usertable").insert({"future": "Alicloud no.1"})
    read_query = r.db('workload').table("usertable").get(1)

    def exec_transaction(opts_cnt):
        for _ in range(opts_cnt):
            write_query.run(conn)
            read_query.run(conn)

    for opts_cnt in range(50):
        latency = []

        start_time = time.time()
        exec_transaction(opts_cnt)
        latency.append(time.time() - start_time)

        # records stats
        median_latency.append(median(latency) * 1000)
        throughput.append(opts_cnt / (latency[-1]))

        print("throughput: {} median_latency: {}".format(str(throughput[-1]), str(median_latency[-1])))

    r.db('workload').table('usertable').delete().run(conn)


def plot_result():
    plt.plot(throughput, median_latency, 'bo')
    plt.xlabel('Throughput (Tx/sec)')
    plt.ylabel('Median Latency (ms)')
    plt.savefig('benchmark.png')


if __name__ == "__main__":
    # start_server()
    # time.sleep(5)

    # db_init()

    execute_write_read_queries()

    plot_result()

    pass

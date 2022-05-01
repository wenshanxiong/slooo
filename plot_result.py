from cProfile import label
from statistics import median
import matplotlib.pyplot as plt
import ast
import os
import sys

leader_results_path = "./results/rethink_leader_swapoff_disk_250_results/"
follower_results_path = "./results/rethink_follower_swapoff_disk_250_results/"

prefix = {"1": "cpu_slow",
          "2": "cpu_contention",
          "3": "disk_slow",
          "4": "disk_contention",
          "5": "memory_contention",
          "6": "network_slow",
          "noslow": "noslow",
          "kill": "crash"}

def parse_file(filename):
    throughput, latency = [], []
    with open(os.path.join(follower_results_path, filename), 'r') as f:
        for line in f.readlines():
            line = line.replace("throughput", "'throughput'")
            line = line.replace("median_latency", "'median_latency'")
            stat = ast.literal_eval(line)
            throughput.append(stat['throughput'])
            latency.append(stat['median_latency'])
    return throughput, latency


def plot_result(stat_dict):
    for k in stat_dict:
        T, L = stat_dict[k]
        if type == "level":
            plt.plot(T[0], L[0], label=k)
        else:
            plt.plot(T[0], L[0], label=prefix[k.split('_')[0]])
    plt.legend()
    plt.title("Follower Fail-Injection comparisons")
    plt.xlabel('Throughput (Tx/sec)')
    plt.ylabel('Median Latency (ms)')
    plt.savefig("./plots/{}.png".format(type))


if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python3 plot_result.py [follower/leader]")
        exit(1)

    stat_dict = {}

    type = sys.argv[1]
    if type == "leader":
        result_path = leader_results_path
    else:
        result_path = follower_results_path

    for filename in os.listdir(result_path):
        throughput, median_latency = parse_file(filename)
        stat_dict[filename] = [[], []]
        stat_dict[filename][0].append(throughput)
        stat_dict[filename][1].append(median_latency)

    plot_result(stat_dict)
import os
from utils.common_utils import *
from multiprocessing import Process
from faults.fault_inject import fault_inject

class Quorum:
    def __init__(self, **kwargs):
        self.opt = kwargs.get("opt")
        self.ondisk = self.opt.ondisk
        self.nodes = config_parser(self.opt.server_configs)
        self.server_configs = self.nodes["servers"]
        self.server_pids = [] # collect pids to check process status
        self.client_configs = self.nodes["client"]
        self.workload = self.opt.workload
        self.threads = self.opt.threads
        self.runtime = self.opt.runtime
        self.exp = kwargs.get("exp")
        self.exp_type = self.opt.exp_type
        self.swap = False  #change this if using memory instead of disk
        self.trial = kwargs.get("trial")
        self.output_path=self.opt.output_path
        self.fault_snooze=int(self.opt.fault_snooze)
        self.primaryip = None
        self.primaryhost = None
        self.fault_server_config = None
        self.fault_pids = None
        self.fault_level = kwargs.get("fault_level")

    def color_print(self, s):
        print('\033[92m' + s + '\033[0m')

    def server_setup(self):
        self.color_print("[server_setup]")
        # if self.ondisk == "disk":
        #     init_disk(self.server_configs, self.exp)
        # elif self.ondisk == "mem":
        #     init_memory(self.server_configs)
        # set_swap_config(self.server_configs, self.swap)

    def start_db(self):
        self.color_print("[start_db]")
        pass

    def db_init(self):
        self.color_print("[db_init]")
        pass

    def benchmark_load(self):
        self.color_print("[benchmark_load]")
        pass

    def benchmark_run(self):
        self.color_print("[benchmark_run]")
        pass

    def db_cleanup(self):
        self.color_print("[db_cleanup]")
        pass

    def server_cleanup(self):
        self.color_print("[server_cleanup]")
        cleanup(self.server_configs, self.swap)

    def run(self):
        self.color_print("Start running fault injection test......")
        is_crash = False

        if hasattr(self, "pd_configs"):
            start_servers(self.server_configs + [self.pd_configs])
        else:
            start_servers(self.server_configs)

        self.server_cleanup()
        self.server_setup()
        self.start_db()
        sleep 10

        self.db_init()
        sleep 10

        self.benchmark_load()
        
        sleep 10

        self.fault_process = Process(target=fault_inject, args=(self.exp, self.fault_server_config, self.fault_pids, self.fault_snooze, self.fault_level, ))
        self.fault_process.start()

        self.benchmark_run()

        self.fault_process.join()
        sleep 15

        if self.opt.point_break:
            for pid in self.server_pids:
                if isinstance(pid, str): pid = int(pid)
                try:
                    os.kill(pid, 0)
                except Exception:
                    print(111)
                    is_crash = True

        self.db_cleanup()
        sleep 5

        self.server_cleanup()
        sleep 5

        if hasattr(self, "pd_configs"):
            stop_servers(self.server_configs + [self.pd_configs])
        else:
            stop_servers(self.server_configs)

        return is_crash


    def cleanup(self):
        self.color_print("[cleanup]")
        start_servers(self.server_configs)
        self.server_cleanup()
        stop_servers(self.server_configs)

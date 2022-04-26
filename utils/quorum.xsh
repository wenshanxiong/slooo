from utils.common_utils import *

class Quorum:
    def __init__(self, **kwargs):
        opt = kwargs.get("opt")
        self.ondisk = opt.ondisk
        self.nodes = config_parser(opt.server_configs)
        self.server_configs = self.nodes["servers"]
        self.client_configs = self.nodes["client"]
        self.workload = opt.workload
        self.threads = opt.threads
        self.runtime = opt.runtime
        self.exp = kwargs.get("exp")
        self.exp_type = opt.exp_type
        self.swap = False  #change this if using memory instead of disk
        self.trial = kwargs.get("trial")
        self.output_path=opt.output_path
        self.fault_snooze=int(opt.fault_snooze)
        self.primaryip = None
        self.primaryhost = None
        self.fault_server_config = None
        self.fault_pids = None

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
        self.color_print("[run]")
        start_servers(self.server_configs)

        self.server_cleanup()

        self.server_setup()
        self.start_db()
        self.db_init()

        self.benchmark_load()
        
        sleep 10

        fault_inject(self.exp, self.fault_server_config, self.fault_pids)

        self.benchmark_run()

        self.server_cleanup()

        stop_servers(self.server_configs)

    def cleanup(self):
        self.color_print("[cleanup]")
        start_servers(self.server_configs)
        self.server_cleanup()
        stop_servers(self.server_configs)

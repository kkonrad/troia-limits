import logging
import requests
import csv
import sys

log = logging.getLogger("storage_performance")
ch = logging.StreamHandler()
ch.setLevel(logging.INFO)
log.addHandler(ch)
log.setLevel(logging.INFO)


from calculate_limits import get_configs, save_results, work_on, ALGORITHM, SIMULATION, TROIA_ADDRESS
from client.galc import TroiaContClient
from client.gal import TroiaClient

ASSIGNS_NUM = 500
JOB_STORAGES = ["MEMORY_FULL", "MEMORY_KV", "DB_FULL", "DB_KV_MEMCACHE_JSON", "DB_KV_MEMCACHE_SIMPLE", "DB_KV_JSON", "DB_KV_SIMPLE"] 
TRIALS = 3

def main(args):
    assert TRIALS >= 3
    configs = get_configs(ASSIGNS_NUM)
    with open('performance.csv', "w") as csv_file:
        results_writer = csv.writer(csv_file, delimiter='\t')
        results_writer.writerow(["{} assigns".format(ASSIGNS_NUM)] + [config[ALGORITHM] for config in configs])
        for js in JOB_STORAGES:
            log.info("--" + js)
            requests.post("{}/config".format(TROIA_ADDRESS), data={'JOBS_STORAGE': js})
            requests.post("{}/config/resetDB".format(TROIA_ADDRESS))
            values = [js]
            for config in configs:
                times = []
                for i in xrange(TRIALS):
                    alg = config[ALGORITHM]
                    log.info("STARTED: %s", alg)
                    res = work_on(config[SIMULATION])
                    computation_time = res['COMPUTE'] + res['UPLOAD']
                    log.info("COMPUTATION TIME: %f", computation_time)
                    log.info("DONE: %s", alg)
                    times.append(computation_time)
                values.append(sum(sorted(times)[1:-1])/(TRIALS-2))
            results_writer.writerow(values)


if __name__ == '__main__':
    main(sys.argv[1:])

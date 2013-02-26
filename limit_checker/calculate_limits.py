import random
import sys
import json
import logging
logging.basicConfig(filename='calculating_limits.log',level=logging.DEBUG)

from troia_cont_client.contClient import TroiaContClient
from troia_client.client import TroiaClient

ASSIGN_PACKAGE_SIZE = 10000
TROIA_ADDRESS = 'http://localhost:8080/troia-server-0.8'

ALGORITHM = "algorithm"
SIMULATION = "simulation"
DATAGENERATOR = "data_generator"

ASSIGNS_OBJECT_RATIO = 5
exps = (3, 4)#4, 5, 6)
ASSIGNS_NUMS = [10 ** x for x in exps]


class DataGenerator(object):

    def __init__(self, num_assigns):
        self.num_assigns = num_assigns
        num_objects = num_assigns / ASSIGNS_OBJECT_RATIO
        self.num_objects = num_objects
        self.num_golds = int(num_objects * 0.05)
        self.num_workers = int(num_objects / 10)

    def gen_init_data(self):
        return ([], {})

    def gen_gold_objects(self):
        return [(self.gen_object(i), self.rand_gold_label())
            for i in xrange(self.num_golds)]

    def rand_label(self):
        pass

    def rand_gold_label(self):
        return self.rand_label()

    def gen_object(self, i):
        return "Object_%i_AAAABBBBBCCCC" % i

    def gen_worker(self, i):
        return "Worker_%i_AAAABBBBBCCCC" % i

    def rand_id(self, limit):
        return random.randint(0, limit - 1)

    def rand_worker(self):
        return self.gen_worker(self.rand_id(self.num_workers))

    def rand_object(self):
        return self.gen_object(self.rand_id(self.num_workers))

    def gen_assign(self):
        return (self.rand_worker(), self.rand_object(), self.rand_label())

    def gen_assigns_packages(self, package_size=ASSIGN_PACKAGE_SIZE):
        N = self.num_assigns // package_size
        M = self.num_assigns % package_size
        for i in xrange(N + int(M > 0)):
            psize = package_size if i < N else M
            yield [self.gen_assign() for _ in xrange(psize)]


class NominalDataGenerator(DataGenerator):

    LABELS = ['correct', 'incorrect']

    def gen_init_data(self):
        return ([
            {"prior":0.5, "name": "correct", "misclassificationCost": [
                {'categoryName': 'correct', 'value': 0},
                {'categoryName': 'incorrect', 'value': 1}
            ]},
            {"prior":0.5, "name": "incorrect", "misclassificationCost": [
                {'categoryName': 'correct', 'value': 1},
                {'categoryName': 'incorrect', 'value': 0}
            ]}
            ], {})

    def rand_label(self):
        return random.choice(self.LABELS)


class ContDataGenerator(DataGenerator):

    def rand_label(self):
        return random.random()

    def rand_gold_label(self):
        return (random.random(), random.random() - 0.5)


class Simulation(object):

    def __init__(self, data_generator):
        self.tc = None
        self.data_generator = data_generator

    def create(self):
        pass

    def prepare(self):
        golds = self.data_generator.gen_gold_objects()
        self._upload_golds(golds)

    def _upload_golds(self, golds):
        pass

    def upload_assigns(self):
        time_server = 0.
        for assigns in self.data_generator.gen_assigns_packages():
            time_server += self._upload_assigns_package(assigns)
        return time_server

    def _upload_assigns_package(self, assigns):
        pass

    def compute(self):
        pass

    def shutdown(self):
        pass


def ret_exectime(tc, resp):
    return tc.await_completion(resp)["executionTime"]


class NominalSimulation(Simulation):

    def __init__(self, algorithm, *args, **kwargs):
        super(NominalSimulation, self).__init__(*args, **kwargs)
        self.algorithm = algorithm

    def create(self):
        self.tc = TroiaClient(TROIA_ADDRESS)
        self.tc.create()

    def _upload_golds(self, golds):
        pass

    def _upload_assigns_package(self, assigns):
        pass

    def compute(self):
        pass

    def shutdown(self):
        pass


class ContSimulation(Simulation):

    def create(self):
        self.tc = TroiaContClient(TROIA_ADDRESS)
        self.tc.createNewJob()

    def _upload_golds(self, golds):
        alt_golds = ((obj, val, zeta) for obj, (val, zeta) in golds)
        return ret_exectime(self.tc, self.tc.post_gold_data(alt_golds))

    def _upload_assigns_package(self, assigns):
        return ret_exectime(self.tc, self.tc.post_assigned_labels(assigns))

    def compute(self):
        return ret_exectime(self.tc, self.tc.post_compute())

    def shutdown(self):
        self.tc.delete()


def get_configs(num_objects):
    cdg = ContDataGenerator(num_objects)
    ndg_ids = NominalDataGenerator(num_objects)
    ndg_bds = NominalDataGenerator(num_objects)

    return [{
        ALGORITHM: 'GALC',
        DATAGENERATOR: cdg,
        SIMULATION: ContSimulation(cdg),
    }]


def work_on(simulation):
    simulation.create()
    simulation.prepare()
    upload_time = simulation.upload_assigns()
    compute_time = simulation.compute()
    simulation.shutdown()
    return {
            "UPLOAD": upload_time,
            "COMPUTE": compute_time,
        }


def save_results(sim_results, algorithm, num_assigns):
    res = {
        "ALGORITHM": algorithm,
        "NUM_ASSIGNS": num_assigns
    }
    res.update(sim_results)
    with open("%s.%s.json" % (num_assigns, algorithm), 'w') as F:
        json.dump(res, F)
    print res
    return res


def main(args):
    for num_assigns in ASSIGNS_NUMS:
        configs = get_configs(num_assigns)
        for config in configs:

            res = work_on(config[SIMULATION])
            save_results(res, config[ALGORITHM], num_assigns)


if __name__ == '__main__':
    main(sys.argv[1:])

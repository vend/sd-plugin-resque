#!/usr/bin/env python
#
# Resque Server Density Plugin
#
# @author morgan@vendhq.com
# 
# Forked from:
#   https://github.com/cwholt/sd-plugin-resque

# Subsequently inspired by:
#   https://github.com/scalarium/server-density-plugins/blob/master/ResqueQueues.py

# Added ability to configure redis-cli path and Resque namespace
# Added derived totals for processed and failed job counts
#

import re
import commands
import time

class Resque:
    def __init__(self, agent_config, checks_logger, raw_config):
        self.agent_config = agent_config
        self.checks_logger = checks_logger
        self.raw_config = raw_config
        self.long_term_stats = {}

        redis_cli = "redis-cli"
        resque_namespace = "resque:"

        if raw_config != None:

            if 'redis_cli' in raw_config['Main']:
                redis_cli = raw_config['Main']['resque_cli']

            if 'resque_namespace' in raw_config['Main']:
                resque_namespace = raw_config['Main']['resque_namespace']

        self.resque_processed = "%s --raw get %sstat:processed" % (redis_cli, resque_namespace)
        self.resque_failed    = "%s --raw get %sstat:failed"% (redis_cli, resque_namespace)
        self.resque_queues    = "%s --raw smembers %squeues"% (redis_cli, resque_namespace)
        self.resque_queuelen  = "%s --raw llen %squeue:"% (redis_cli, resque_namespace)

    def run(self):
        stats = {}
        stats['processed'] = int(commands.getoutput(self.resque_processed))
        stats['failed'] = int(commands.getoutput(self.resque_failed))
        stats['queued'] = 0
        for queue in commands.getoutput(self.resque_queues).splitlines():
            stats[queue] = int(commands.getoutput(self.resque_queuelen + queue))
            stats['queued'] = stats['queued'] + stats[queue]

        derive_stats = ['processed', 'failed']
        for stat in derive_stats:
            if stat in self.long_term_stats:
                derived = stats[stat] - self.long_term_stats[stat]
                self.long_term_stats[stat] = stats[stat]
                stats[stat] = derived
            else:
                self.long_term_stats[stat] = stats[stat]
                stats[stat] = 0

        return stats

if __name__ == '__main__':
    resque = Resque(None, None, None)
    print resque.run()
    time.sleep(30)
    print resque.run()

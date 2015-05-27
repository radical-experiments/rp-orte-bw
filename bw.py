#!/usr/bin/env python

__copyright__ = "Copyright 2015, http://radical.rutgers.edu"
__license__   = "MIT"

agent_config = {
    "number_of_workers": {
        "StageinWorker"   : 1,
        "ExecWorker"      : 8,
        "StageoutWorker"  : 1,
        "UpdateWorker"    : 1
    },
    "blowup_factor": {
        "Agent"           : 1,
        "stagein_queue"   : 1,
        "StageinWorker"   : 1,
        "schedule_queue"  : 1,
        "Scheduler"       : 1,
        "execution_queue" : 1,
        "ExecWorker"      : 1,
        "watch_queue"     : 1,
        "ExecWatcher"     : 1,
        "stageout_queue"  : 1,
        "StageoutWorker"  : 1,
        "update_queue"    : 1,
        "UpdateWorker"    : 1
    },
    "drop_clones": {
        "Agent"           : 1,
        "stagein_queue"   : 1,
        "StageinWorker"   : 1,
        "schedule_queue"  : 1,
        "Scheduler"       : 1,
        "execution_queue" : 1,
        "ExecWorker"      : 1,
        "watch_queue"     : 1,
        "ExecWatcher"     : 1,
        "stageout_queue"  : 1,
        "StageoutWorker"  : 1,
        "update_queue"    : 1,
        "UpdateWorker"    : 1
    }
}

import sys
import time
import os
import radical.pilot as rp

EXPERIMENT = 'ORTE'
#EXPERIMENT = 'APRUN'
#EXPERIMENT = 'CCM'
#EXPERIMENT = 'LOCAL'

NODES = 10
RUNTIME = 30
SLEEP = 0
MULTIPLIER = 5 # In the time dimension
CU_CORES = 1 # Can only be a multiple of 32/single nodes for APRUN

PROFILING=True

RP_VERSION = "debug" # debug, installed, local
VIRTENV_MODE = "create" # create, use, update

#
# XE nodes have 2 "Interlagos" Processors with 8 "Bulldozer" cores each.
# Every "Bulldozer" core consists of 2 schedualable integer cores.
# XE nodes therefore have a PPN=32.
#
# Depending on the type of application,
# one generally chooses to have 16 or 32 instances per node.

config = {
    'LOCAL': {
        'RESOURCE': 'local.localhost',
        'LAUNCH_METHOD': 'FORK',
        #'AGENT_SPAWNER': 'POPEN',
        'AGENT_SPAWNER': 'SHELL',
        'PPN': 4
    },
    'APRUN': {
        'RESOURCE': 'ncsa.bw',
        'LAUNCH_METHOD': 'APRUN',
        'AGENT_SPAWNER': 'SHELL',
        #'AGENT_SPAWNER': 'POPEN',
        'QUEUE': 'debug', # Maximum 30 minutes
        'PPN': 32
    },
    'CCM': {
        'RESOURCE': 'ncsa.bw_ccm',
        #'LAUNCH_METHOD': 'SSH',
        'LAUNCH_METHOD': 'MPIRUN',
        #'AGENT_SPAWNER': 'SHELL',
        'AGENT_SPAWNER': 'POPEN',
        'QUEUE': 'debug', # Maximum 30 minutes
        'PPN': 32,
        'PRE_EXEC_PREPEND': [
            'module use --append /u/sciteam/marksant/privatemodules',
            'module load use.own',
            'module load openmpi/1.8.4_ccm'
        ]
    },
    'ORTE': {
        'RESOURCE': 'ncsa.bw',
        'LAUNCH_METHOD': "ORTE",
        'AGENT_SPAWNER': 'SHELL',
        #'AGENT_SPAWNER': 'POPEN',
        'QUEUE': 'debug', # Maximum 30 minutes
        'PPN': 32,
        'PRE_EXEC_PREPEND': [
            'module use --append /u/sciteam/marksant/privatemodules',
            'module load use.own',
            'module load openmpi/git'
        ]
    }
}

# The number of cores to acquire on the resource
PILOT_CORES = config[EXPERIMENT]['PPN'] * NODES

# Launch enough cores to consume all cores of the pilot for the multiplier duration.
#UNITS = (PILOT_CORES/CU_CORES) * MULTIPLIER
# or a fixed number
UNITS = 500

# Schedule CUs directly to a Pilot, assumes single Pilot
SCHED = rp.SCHED_DIRECT_SUBMISSION

# Profiling
if PROFILING:
    #os.putenv('RADICAL_PILOT_PROFILE','TRUE')
    os.environ['RADICAL_PILOT_PROFILE'] = 'TRUE'

#
# Copy AGENT config
#
# .radical/pilot/configs

#------------------------------------------------------------------------------
#
def pilot_state_cb (pilot, state):

    if not pilot:
        return

    print "[Callback]: ComputePilot '%s' state: %s." % (pilot.uid, state)

    if state == rp.FAILED:
        sys.exit (1)


CNT = 0
#------------------------------------------------------------------------------
#
def unit_state_cb (unit, state):

    if not unit:
        return

    global CNT

    print "[Callback]: unit %s on %s: %s." % (unit.uid, unit.pilot_id, state)

    if state in [rp.FAILED, rp.DONE, rp.CANCELED]:
        CNT += 1
        print "[Callback]: # %6d" % CNT


    if state == rp.FAILED:
        print "stderr: %s" % unit.stderr
        #sys.exit(2)


#------------------------------------------------------------------------------
#
def wait_queue_size_cb(umgr, wait_queue_size):

    print "[Callback]: wait_queue_size: %s." % wait_queue_size


#------------------------------------------------------------------------------
#
if __name__ == "__main__":

    # we can optionally pass session name to RP
    if len(sys.argv) > 1:
        session_name = sys.argv[1]
    else:
        session_name = None

    # Create a new session. No need to try/except this: if session creation
    # fails, there is not much we can do anyways...
    session = rp.Session(name=session_name)
    print "session id: %s" % session.uid

    cfg = session.get_resource_config(config[EXPERIMENT]['RESOURCE'])

    # create a new config based on the old one, and set a different queue
    new_cfg = rp.ResourceConfig(config[EXPERIMENT]['RESOURCE'], cfg)

    # Insert pre_execs at the beginning in reverse order
    if 'PRE_EXEC_PREPEND' in config[EXPERIMENT]:
        for entry in config[EXPERIMENT]['PRE_EXEC_PREPEND'][::-1]:
            new_cfg.pre_bootstrap.insert(0, entry)

    # Change launch method
    new_cfg.task_launch_method = config[EXPERIMENT]['LAUNCH_METHOD']

    # Change method to spawn tasks
    new_cfg.agent_spawner = config[EXPERIMENT]['AGENT_SPAWNER']

    # Don't install a new version of RP
    new_cfg.rp_version = RP_VERSION
    new_cfg.virtenv_mode = VIRTENV_MODE

    # now add the entry back.  As we did not change the config name, this will
    # replace the original configuration.  A completely new configuration would
    # need a unique name.
    session.add_resource_config(new_cfg)

    # all other pilot code is now tried/excepted.  If an exception is caught, we
    # can rely on the session object to exist and be valid, and we can thus tear
    # the whole RP stack down via a 'session.close()' call in the 'finally'
    # clause...
    try:

        pmgr = rp.PilotManager(session=session)
        pmgr.register_callback(pilot_state_cb)

        pdesc = rp.ComputePilotDescription()
        pdesc.resource = config[EXPERIMENT]['RESOURCE']
        pdesc.cores = PILOT_CORES
        if 'QUEUE' in config[EXPERIMENT]:
            pdesc.queue = config[EXPERIMENT]['QUEUE']
        pdesc.runtime = RUNTIME
        pdesc.cleanup = False

        pdesc._config = agent_config

        pilot = pmgr.submit_pilots(pdesc)

        umgr = rp.UnitManager(session=session, scheduler=SCHED)
        umgr.register_callback(unit_state_cb,      rp.UNIT_STATE)
        umgr.register_callback(wait_queue_size_cb, rp.WAIT_QUEUE_SIZE)
        umgr.add_pilots(pilot)

        # Don't want any CUs
        #time.sleep(600)

        cuds = list()
        for unit_count in range(0, UNITS):
            cud = rp.ComputeUnitDescription()
            cud.executable     = "/bin/bash"
            cud.arguments      = ["-c", "date && sleep %d && date" % SLEEP]
            cud.cores          = CU_CORES
            cuds.append(cud)

        units = umgr.submit_units(cuds)
        umgr.wait_units()

        for cu in units:
            print "* Task %s state %s, exit code: %s, started: %s, finished: %s" \
                % (cu.uid, cu.state, cu.exit_code, cu.start_time, cu.stop_time)

      # os.system ("radicalpilot-stats -m stat,plot -s %s > %s.stat" % (session.uid, session_name))

    except Exception as e:
        # Something unexpected happened in the pilot code above
        print "caught Exception: %s" % e
        raise

    except (KeyboardInterrupt, SystemExit) as e:
        # the callback called sys.exit(), and we can here catch the
        # corresponding KeyboardInterrupt exception for shutdown.  We also catch
        # SystemExit (which gets raised if the main threads exits for some other
        # reason).
        print "need to exit now: %s" % e

    finally:

        # Give the agent chance to write all.
        time.sleep(25)

        print "closing session"
        session.close(cleanup=False, terminate=True)

#-------------------------------------------------------------------------------

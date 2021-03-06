#!/usr/bin/python
# -*- coding: utf-8 -*-
# Copyright (C) 2008-2013, Luis Pedro Coelho <luis@luispedro.org>
# vim: set ts=4 sts=4 sw=4 expandtab smartindent:
#
# Permission is hereby granted, free of charge, to any person obtaining a copy
#  of this software and associated documentation files (the "Software"), to deal
#  in the Software without restriction, including without limitation the rights
#  to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
#  copies of the Software, and to permit persons to whom the Software is
#  furnished to do so, subject to the following conditions:
#
# The above copyright notice and this permission notice shall be included in
#  all copies or substantial portions of the Software.
#
# THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
#  IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
#  FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
#  AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
#  LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
#  OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN
#  THE SOFTWARE.


from collections import defaultdict, Counter
import sys
import os
import os.path
import re
import logging
logger = logging.getLogger(__name__)

from . import backends
from .task import Task, Tasklet, walk_dependencies
from . import task
from .io import print_task_summary_table, render_task_summary_table
from .subcommands.status import status
from .subcommands.webstatus import webstatus
from .subcommands.shell import shell
from .barrier import BarrierError

def do_print(store, options):
    '''
    do_print(store, options)

    Print a count of task names.

    Parameters
    ----------
    store : jug backend
    options : jug options
    '''
    task_counts = defaultdict(int)
    for t in task.alltasks:
        task_counts[t.name] += 1

    print_task_summary_table(options, [("Count", task_counts)])

def invalidate(store, options):
    '''
    invalidate(store, options)

    Implements 'invalidate' command

    Parameters
    ----------
    store : jug.backend
    options : options object
        Most relevant option is `invalid_name`, a string  with the exact (i.e.,
        module qualified) name of function to invalidate
    '''
    invalid_name = options.invalid_name
    if re.match( r'/.*?/', invalid_name):
        # Looks like a regular expression
        invalidate_re = re.compile( invalid_name.strip('/') )
    elif '.' in invalid_name:
        # Looks like a full task name
        invalidate_re = re.compile(invalid_name.replace('.','\\.' ))
    else:
        # A bare function name perhaps?
        invalidate_re = re.compile(r'\.' + invalid_name )

    logger.info("Created invalidate_re: %r", invalidate_re.pattern)

    invalid_tasks = {}
    tasks = task.alltasks

    # Scan and mark tasks as invalidated
    for t in tasks:
        if re.search( invalidate_re, t.name ):
            invalid_tasks[t.hash()] = t

    if not invalid_tasks:
        options.print_out('No tasks matched invalid pattern.')
        return

    valid_tasks = {}
    def scan_task_dependencies(t):
        for _, deps in walk_dependencies(t):
            for i in reversed(range(len(deps))):
                if deps[i].hash() in invalid_tasks:
                    invalid_tasks[t.hash()] = t
                    return
                elif deps[i].hash() in valid_tasks:
                    deps.pop(i)

        valid_tasks[t.hash()] = True

    for t in tasks:
        scan_task_dependencies(t)

    task_counts = defaultdict(int)
    for t in invalid_tasks.values():
        if not options.dry_run:
            if store.remove(t.hash()):
                task_counts[t.name] += 1
        else:
            if store.can_load(t.hash()):
                task_counts[t.name] += 1

    if sum(task_counts.values()) == 0:
        options.print_out('Tasks matched invalid pattern, but no results present.')
    else:
        print_task_summary_table(options, [("Invalidated", task_counts)])

def _sigterm(_,__):
    sys.exit(1)

class Executor(object):
    def __init__(self, store, tasks, execute_wait_cycle_time_secs, aggressive_unload, debug_mode, pdb, execute_keep_going):
        logger.info("Beginning execution: <%s tasks>", len(tasks))

        self.store = store
        self.tasks = tasks

        self.execute_wait_cycle_time_secs = execute_wait_cycle_time_secs
        self.aggressive_unload            = aggressive_unload
        self.debug_mode                   = debug_mode
        self.pdb                          = pdb
        self.execute_keep_going           = execute_keep_going

    def execute_loop(self, execute_nr_wait_cycles):
        from time import sleep

        wait_cycles = int(execute_nr_wait_cycles)

        tasks_current  = self.tasks
        tasks_total_executed = []
        tasks_finished = []

        while tasks_current:
            tasks_waiting  = []
            tasks_ready    = []
            tasks_locked   = []
            tasks_executed = []

            for t in tasks_current:
                if t.can_load():
                    tasks_finished.append(t)
                elif t.is_locked():
                    tasks_locked.append(t)
                elif t.can_run():
                    tasks_ready.append(t)
                else:
                    tasks_waiting.append(t)

            task_summary_table = render_task_summary_table(
                    [
                        ("waiting" , Counter( [t.display_name for t in tasks_waiting])),
                        ("ready" , Counter( [t.display_name for t in tasks_ready])),
                        ("locked" , Counter( [t.display_name for t in tasks_locked])),
                        ("finished" , Counter( [t.display_name for t in tasks_finished])),
                    ])
            logger.info("Pre-execute task status:\n" + "\n".join(task_summary_table))

            for t in tasks_ready:
                # Check if task is loadable. If so discard from task list.
                # Lock task for execution.
                # If loadable due to execution during lock operation discard from task list.
                # If lock failed push task back onto tasks queue.
                # If locked then execute.
                if t.can_load():
                    tasks_finished.append(t)
                    continue

                locked = False
                try:
                    locked = t.lock()

                    if t.can_load():
                        tasks_finished.append(t)
                        continue
                    elif not locked:
                        tasks_locked.append(t)
                    else:
                        executed = self.execute_task(t)
                        if executed:
                            # Executed is false only if exception was generated and ignored
                            # during task execution.
                            # In which case task should be dropped from the task list.
                            tasks_executed.append(t)
                finally:
                    if locked:
                        t.unlock()

            tasks_total_executed.extend(tasks_executed)
            tasks_current = tasks_waiting + tasks_locked

            if tasks_current and not tasks_executed:
                if wait_cycles > 0:
                    wait_cycles -= 1
                    logger.info("Waiting %s seconds for open task. wait_cycle: %s/%s", self.execute_wait_cycle_time_secs, wait_cycles, execute_nr_wait_cycles)
                    sleep(int(self.execute_wait_cycle_time_secs))
                else:
                    logger.info("Finished wait cycles without open task.")
                    return tasks_executed

        logger.info("No tasks available to run.")
        return tasks_total_executed

    def execute_task(self, task):
        try:
            logger.info("Begin task: %s", task.display_name)
            task.run(debug_mode = self.debug_mode)
            logger.info("Ended task: %s", task.display_name)
            if self.aggressive_unload:
                task.unload_recursive()
            return True

        except (Exception, KeyboardInterrupt) as e:
            if self.pdb:
                exc_info = sys.exc_info()
                try:
                    from IPython.core import ultratb
                    ultratb.FormattedTB(call_pdb=1)( exc_info )
                except ImportError:
                    #Fallback to standard debugger
                    _, _, tb = exc_info
                    import pdb
                    pdb.post_mortem(tb)
            else:
                logger.critical('Exception while running %s: %s', task.name, e)

            if self.execute_keep_going:
                return False
            else:
                raise

def inline_execute(tasks, store=None, aggressive_unload=False):
    if store is None:
        store = Task.store

    tasks = list(task.tasks_for_value(tasks))

    executor = Executor(
            store, tasks,
            execute_wait_cycle_time_secs = 0,
            aggressive_unload = aggressive_unload,
            debug_mode = False,
            pdb = False,
            execute_keep_going = False,
        )

    tasks_executed_in_cycle = executor.execute_loop(0)

def execute(options):
    '''
    execute(options)

    Implement 'execute' command
    '''
    from signal import signal, SIGTERM

    signal(SIGTERM,_sigterm)

    tasks = task.alltasks

    tasks_executed = defaultdict(int)
    store = None

    from time import sleep
    wait_cycles = int(options.execute_nr_wait_cycles)

    while wait_cycles > 0:
        del tasks[:]
        store, jugspace = init(options.jugfile, options.jugdir, store=store)
        if options.debug:
            for t in tasks:
                # Trigger hash computation:
                t.hash()

        has_barrier = jugspace.get('__jug__hasbarrier__', False)
        executor = Executor(
                store,
                tasks,
                options.execute_wait_cycle_time_secs,
                options.aggressive_unload,
                options.debug,
                options.pdb,
                options.execute_keep_going)

        tasks_executed_in_cycle = executor.execute_loop(0 if has_barrier else int(options.execute_nr_wait_cycles))

        for t in tasks_executed_in_cycle:
            tasks_executed[t.display_name] += 1

        if not has_barrier:
            break

        if not tasks_executed_in_cycle:
            wait_cycles -= 1
            logger.info("Waiting %s seconds to recycle barrier.", options.execute_wait_cycle_time_secs)
            sleep(int(options.execute_wait_cycle_time_secs))
    else:
        logger.info('Execute ending, no tasks can be run.')

    print_task_summary_table(options, [("Executed", tasks_executed)])

def cleanup(store, options):
    '''
    cleanup(store, options)

    Implement 'cleanup' command
    '''
    if options.cleanup_locks_only:
        logging.info("cleanup: cleanup_locks_only")
        removed = store.remove_locks()
    else:
        logging.info("cleanup: cleanup")
        tasks = task.alltasks
        removed = store.cleanup(tasks)

    options.print_out('Removed %s files' % removed)

def check(store, options):
    '''
    check(store, options)

    Executes check subcommand

    Parameters
    ----------
    store : jug.backend
            backend to use
    options : jug options
    '''
    sys.exit(_check_or_sleep_until(store, False))

def sleep_until(store, options):
    '''
    sleep_until(store, options)

    Execute sleep-until subcommand

    Parameters
    ----------
    store : jug.backend
            backend to use
    options : jug options
        ignored
    '''
    sys.exit(_check_or_sleep_until(store, True))

def _check_or_sleep_until(store, sleep_until):
    from .task import recursive_dependencies
    tasks = task.alltasks
    active = set(tasks)
    for t in reversed(tasks):
        if t not in active:
            continue
        while not t.can_load(store):
            if sleep_until:
                from time import sleep
                sleep(12)
            else:
                return 1
        else:
            for dep in recursive_dependencies(t):
                try:
                    active.remove(dep)
                except KeyError:
                    pass
    return 0

def init(jugfile="jugfile", jugdir=None, on_error='exit', store=None):
    '''
    store,jugspace = init(jugfile={'jugfile'}, jugdir={'jugdata'}, on_error='exit', store=None)

    Initializes jug (create backend connection, ...).
    Imports jugfile

    Parameters
    ----------
    jugfile : str, optional
        jugfile to import (default: 'jugfile')
    jugdir : str, optional
        jugdir to use (could be a path)
    on_error : str, optional
        What to do if import fails (default: exit)
    store : storage object, optional
        If used, this is returned as ``store`` again.

    Returns
    -------
    store : storage object
    jugspace : dictionary
    '''
    import imp
    from .options import resolve_jugdir
    assert on_error in ('exit', 'propagate'), 'jug.init: on_error option is not valid.'

    logger.debug("init(jugfile=%s, jugdir=%r, on_error=%s, store=%s)", jugfile, jugdir, on_error, store)

    if jugdir is None:
        jugdir = resolve_jugdir(jugfile)

    if store is None:
        store = set_jugdir(jugdir)

    sys.path.insert(0, os.path.abspath('.'))

    # The reason for this implementation is that it is the only that seems to
    # work with both barrier and pickle()ing of functions inside the jugfile
    #
    # Just doing __import__() will not work because if there is a BarrierError
    # thrown, then functions defined inside the jugfile end up in a confusing
    # state.
    #
    # Alternatively, just execfile()ing will make any functions defined in the
    # jugfile unpickle()able which makes mapreduce not work
    #
    # Therefore, we simulate (partially) __import__ and set sys.modules *even*
    # if BarrierError is raised.
    #
    jugmodname = os.path.basename(jugfile[:-len('.py')])
    jugmodule = imp.new_module(jugmodname)
    jugmodule.__file__ = os.path.abspath(jugfile)
    jugspace = jugmodule.__dict__
    sys.modules[jugmodname] = jugmodule
    jugfile_contents = open(jugfile).read()
    try:
        exec(compile(jugfile_contents, jugfile, 'exec'), jugspace, jugspace)
    except BarrierError:
        jugspace['__jug__hasbarrier__'] = True
    except Exception as e:
        logger.critical("Could not import file '%s' (error: %s)", jugfile, e)
        if on_error == 'exit':
            import traceback
            print(traceback.format_exc())
            sys.exit(1)
        else:
            raise

    # The store may have been changed by the jugfile.
    store = Task.store
    return store, jugspace

def set_jugdir(jugdir):
    '''
    store = set_jugdir(jugdir)

    Sets the jugdir. This is the programmatic equivalent of passing
    ``--jugdir=...`` on the command line.

    Parameters
    ----------
    jugdir : str

    Returns
    -------
    store : a jug backend
    '''
    store = backends.select(jugdir)
    Task.store = store
    return store

def main(argv=None):
    from .options import parse
    if argv is None:
        from sys import argv
    options = parse(argv[1:])
    store = None
    if options.cmd not in ('status', 'execute', 'webstatus'):
        store,jugspace = init(options.jugfile, options.jugdir)

    if options.cmd == 'execute':
        execute(options)
    elif options.cmd == 'count':
        do_print(store, options)
    elif options.cmd == 'check':
        check(store, options)
    elif options.cmd == 'sleep-until':
        sleep_until(store, options)
    elif options.cmd == 'status':
        status(options)
    elif options.cmd == 'invalidate':
        invalidate(store, options)
    elif options.cmd == 'cleanup':
        cleanup(store, options)
    elif options.cmd == 'shell':
        shell(store, options, jugspace)
    elif options.cmd == 'webstatus':
        webstatus(options)
    else:
        logger.critical('Jug: unknown command: \'%s\'' % options.cmd)
    if store is not None:
        store.close()

if __name__ == '__main__':
    try:
        main()
    except Exception as exc:
        logger.critical('Unhandled Jug Error!')
        raise

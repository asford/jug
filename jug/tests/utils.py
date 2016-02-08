from .task_reset import task_reset

def simple_execute(tasks = None, options = None):
    from jug.jug import Executor
    from jug.task import alltasks, Task
    from jug.options import default_options

    if options is None:
        options = default_options

    if tasks is None:
        tasks = list(alltasks)

    executor = Executor(
            Task.store,
            tasks,
            options.execute_wait_cycle_time_secs,
            options.aggressive_unload,
            options.debug,
            options.pdb,
            options.execute_keep_going)
    return executor.execute_loop( options.execute_nr_wait_cycles )

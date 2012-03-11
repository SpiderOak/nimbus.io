import os
import time
import Queue

_fsync_task_interval = float(os.environ.get("NIMBUSIO_FSYNC_INTERVAL", "1.0"))

class FsyncNotifyWatcher(object):

    def __init__(self, halt_event, queue, complete, dipatch):
        self._halt_event = halt_event
        self._queue = queue
        # set of IDs of tasks already done
        self._complete = task_complete 
        # map of IDs of waiting task to tasks to queue when they complete
        self._dispatch = task_dipatch  

    def run(self):
        # TODO handle notifications 

        complete_ids = []
        queue_tasks = []

        try:
            while True:
                notify = self._queue.pop(False)
                # TODO accumulate:
                # form ( 'tasks-ids-complete', LIST_OF_IDS, )
                # form ( 'queue-tasks', FUNCTION, ARGS, )
        except Queue.Empty:
            pass

        for task_id in complete_ids:

            if task_id in self._dispatch:
                function, args = self._dispatch[task_id]
                del self._dispatch[task_id]
                function(*args)
            else:
                self._complete.add(task_id)

        for function, args in queue_tasks:
            function(*args)

        # queue our self to run again...
        if complete_ids or queue_tasks:
            return [self.run, time.time(), ]
        else:
            # disappointing latency here...
            return [self.run, time.time() + 0.1, ]

def fsync_task(queue, complete_queue, halt_event):
    """
    every _fsync_task_interval, pop ALL tasks from the queue
    determine the time of the next _fsync_task_interval
    process all the tasks
    put complete notifications onto the complete_queue
    wait sleep until the calculated time unti the next interval
    watch halt event and exit when halt event is set and tasks are empty.
    """
    log = logging.getLogger("fsync_task")

    def _get_tasks(queue):
        "grab all tasks from the queue"
        tasks = []
        try:
            while True:
                tasks.append(queue.pop(False))
        except Queue.Empty:
            pass
        return tasks

    def _process_tasks(tasks, complete_queue):
        "process all tasks in the queue, put notifications on complete_queue"

        if not tasks:
            return
            
        # task IDs to notify by fileno
        filenos = defaultdict(list)
        # tasks to queue when all files are synced
        queue_when_complete = []

        for task in tasks:

            tag = task[0]
            args = task[1:]

            if tag == 'fsync-fileno':
                task_id, fileno = args
                filenos[fileno].append(task_id)
            elif tag == 'queue-when-all-fsyncs-complete':
                queue_when_complete.append(args[0])
            else:
                log.error("unknown task tag: %r" % (tag, ))

        # sort files, fsync them, and notify everything waiting on them.
        for fileno in sorted(filenos.keys()):
            os.fsync(fileno)
            complete_queue.append(('tasks-ids-complete', filenos[fileno], ))

        # if there were any callables to be ran in main thread, queue those.
        if queue_when_complete:
            complete_queue.append(('queue-tasks', queue_when_complete, ))

    while True:
        loop_start_time = time.time()
        next_loop_time = loop_start_time + _fsync_task_interval
        tasks = _get_all_tasks()
        if not tasks and halt_event.is_set():
            return
        _process_all_tasks(tasks, complete_queue)
        sleep_time = next_loop_time - time.time
        if sleep_time > 0.0:
            halt_event.wait(sleep_time)


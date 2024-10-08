


import random
import time
import asyncio
from pathlib import Path

from ..task import *

#from .tasks import task_rmtree
#from .task import task, task_graph
from ..runner.main import runTaskGraph


def printFn(*strs, **extras):
    st = 2
    #st = random.random() * 5
    #random.seed(time.time())
    time.sleep(st)
    r = f'Sleep {st}s in {strs[0]}.'
    for s in strs[1:]:
        r += ' '
        r += s
    for i, es in extras.items():
        r += ' '.join(str(s) for s in es)
    #a = 1 / random.randint(0, 1)
    print(r)
    return True, r

def visitFile(filePath: str):
    try:
        bstr = Path(filePath).read_bytes()
        print(f'File "{filePath}" size: {len(bstr)}.')
        return True
    except:
        return False

def runTestTaskGraph():
    #testPath = Path('1')
    #testPath.mkdir(exist_ok=True)
    #testText = testPath.joinpath('1.txt')
    #testText.touch()

    taskDefs = tuple(
        TaskDef (
            task=Task(f'run task {i}', fn=printFn),
            context=TaskContext(args=(f'task{i}',)),
            pipeType=EPipeType.default if i>1 and i!=5 else EPipeType.none
        ) for i in range(10)
    )
    taskDefs[2].precursors = 0
    taskDefs[3].precursors = 1
    taskDefs[4].precursors = (2, 1)
    taskDefs[5].precursors = (3, 8)
    taskDefs[6].precursors = 4
    taskDefs[7].precursors = 0
    taskDefs[8].precursors = 6
    taskDefs[9].precursors = 5
    taskGraph = TaskGraph({
        0: taskDefs[0],
        1: taskDefs[1],
        2: taskDefs[2],
        3: taskDefs[3],
        4: taskDefs[4],
        6: taskDefs[6],
        8: taskDefs[8],

        5: taskDefs[5],
        9: taskDefs[9],

        7: taskDefs[7],
    })
    taskGraph.printGraph()

    taskGraph1 = TaskGraph({
        'main': TaskDef (
            task=Task(
                'run task on main', fn=lambda : print('On main process')
            ),
            runType=ERunType.synchronous
        ),
        'main_test': TaskDef(
            task=Task('do mp test', fn=visitFile),
            context=TaskContext(
                argsForMap=(str(p) for p in Path('.').glob('**/*.py'))
            ),
            runType=ERunType.synchronous
        ),
    })
    taskGraph = taskGraph.merge(taskGraph1, {
        0: 'main',
        1: 'main',
    })
    taskGraph.printGraph()

def runTestSyncOnly():
    # test1: all synchronous single tasks
    taskDefs = tuple(
        TaskDef (
            task=Task(f'run task {i}', fn=printFn),
            context=TaskContext(args=(f'task{i}',)),
            runType=ERunType.synchronous,
        ) for i in range(4)
    )
    taskDefs[2].precursors = 1
    taskDefs[2].pipeTypes = {1:EPipeType.default}
    taskGraph = TaskGraph({
        i: taskDefs[i] for i in range(4)
    })
    startTime = time.monotonic()
    runTaskGraph(taskGraph)
    sleepTime = time.monotonic() - startTime
    print(f'run order: 0 1 3 2')
    print(f'total sleep time: {sleepTime:.2f} seconds')
    print(f'total expected sleep time: 8 seconds')

    # test2: all single tasks on mp
    



'''
    fileTaskGraph = task_rmtree.QuickRmTreeGraph(testPath)
    fileTaskGraph.printGraph()

    tasks = tuple(
        task.SingleTask(
            f'run task {i}', fn=printFn, args=(f'task{i}', ), pipe=i>1 and i!=5, needMp=True)\
                for i in range(10)
    )
    taskGraph1 = task_graph.TaskGraph({
        'main': task.SingleTask(
            'run task on main', fn=lambda : print('On main process')==None
        ),
        'main_test': task.MpTask(
            'do mp test', fn=visitFile,
            argsForMp=(str(p) for p in Path('.').glob('**/*.py'))
        ),
    })
    taskGraph1.printGraph()
    taskGraph = task_graph.TaskGraph({
        0: tasks[0],
        1: tasks[1],
        2: (tasks[2], 0),
        3: (tasks[3], 1),
        4: (tasks[4], (2, 1)),
        6: (tasks[6], 4),
        8: (tasks[8], 6),

        5: (tasks[5], (3, 8)),
        9: (tasks[9], 5),

        7: (tasks[7], 0),
    })
    taskGraph.printGraph()
    taskGraph = taskGraph.merge(taskGraph1, {
        0: 'main',
        1: 'main',
    })
    taskGraph.printGraph()

    taskGraph = taskGraph.merge(fileTaskGraph, {
        fileTaskGraph.startId: ('main_test', 6),
        5: fileTaskGraph.startId,
        #9: fileTaskGraph.startId,       # error test, pipe task
        #8: 'main',                      # error test, add new dep
        #1: fileTaskGraph.startId,       # error test, circular dependency
    })
    taskGraph.printGraph()

    taskGraph = task_graph.TaskGraph({
        taskGraph1.startId: task_graph.SubGraphTask(graph=taskGraph1),
        0: (tasks[0], 'main'),
        1: (tasks[1], 'main'),
        2: (tasks[2], 0),
        3: (tasks[3], 1),
        4: (tasks[4], (2, 1)),
        6: (tasks[6], 4),
        8: (tasks[8], 6),

        5: (tasks[5], (3, 8)),
        9: (tasks[9], 5), 

        7: (tasks[7], 0),

        fileTaskGraph.startId: (
            task_graph.SubGraphTask(
                graph=fileTaskGraph,
                extraDeps={5: fileTaskGraph.startId}
            ),
            ('main_test', 6)
        ),
    })
    taskGraph.printGraph()
    taskGraph = taskGraph.copy()
    taskGraph.printGraph()

    main_runner.runTaskGraph(taskGraph)
'''

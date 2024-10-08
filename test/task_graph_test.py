


import unittest
from pathlib import Path

from .test_common import printFn, printFnAsync
from ..task import *


class TaskGraphTest(unittest.TestCase):

    def test_01_emptyTask(self):
        emptyTask = Task(f'Empty')
        syncTask = Task(f'Sync', fnCoroutine=printFn)
        asyncTask = Task(f'Async', fnCoroutine=printFnAsync)

        with self.assertRaises(AssertionError):
            taskGraph = TaskGraph({
                0: TaskDef(task=emptyTask, runType=ERunType.synchronous)
            })
        with self.assertRaises(AssertionError):
            taskGraph = TaskGraph({
                0: TaskDef(task=emptyTask, runType=ERunType.mp)
            })
        with self.assertRaises(AssertionError):
            taskGraph = TaskGraph({
                0: TaskDef(task=emptyTask, runType=ERunType.coroutine)
            })
        
        taskGraph = TaskGraph({
            0: TaskDef(task=syncTask, runType=ERunType.synchronous)
        })
        taskGraph = TaskGraph({
            0: TaskDef(task=syncTask, runType=ERunType.mp)
        })
        taskGraph = TaskGraph({
            0: TaskDef(task=syncTask, runType=ERunType.coroutine)
        })

        with self.assertRaises(AssertionError):
            taskGraph = TaskGraph({
                0: TaskDef(task=asyncTask, runType=ERunType.synchronous)
            })
        with self.assertRaises(AssertionError):
            taskGraph = TaskGraph({
                0: TaskDef(task=asyncTask, runType=ERunType.mp)
            })
        taskGraph = TaskGraph({
            0: TaskDef(task=asyncTask, runType=ERunType.coroutine)
        })

'''
def runTestTaskGraph():
    #testPath = Path('1')
    #testPath.mkdir(exist_ok=True)
    #testText = testPath.joinpath('1.txt')
    #testText.touch()

    taskDefs = tuple(
        TaskDef (
            task=Task(f'run task {i}', fn=printFn),
            context=TaskContext(args=(2.0, f'task{i}',)),
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
'''

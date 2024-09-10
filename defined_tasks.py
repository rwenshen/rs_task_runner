
@dataclasses.dataclass
class DefinedTaskManager:
    taskDefinitions: Dict[Hashable, TaskDefinition]
    # always to run tasks; These tasks will be call for each task
    alwaysDeps: Iterable[Hashable] = ()
    # root for paths in TaskDefinition
    commonRoot: Path|str|None = None

    def __createGraph(self, taskType: Hashable,
                                    force:bool) -> DefinedTaskGraph:
        taskDefinition: TaskDefinition = self.taskDefinitions[taskType]
        deps = [*taskDefinition.deps,]
        if taskType not in self.alwaysDeps:
            for depTaskType in self.alwaysDeps:
                deps.append(TaskDepDefinition(depTaskType))
        depDict = {}
        for depDef in deps:
            depTaskDef: TaskDefinition = \
                self.taskDefinitions[depDef.taskType]
            if taskDefinition.desc == depTaskDef.desc:
                continue
            depDest = depTaskDef.post_desc\
                        if depDef.dependentToPostTask else depTaskDef.desc
            depGraph = self.__createGraph(depDef.taskType, force)
            depDict[depDest] = depGraph

        return DefinedTaskGraph(taskDefinition, force=force,
                    commonRoot=self.commonRoot, extraDepDict=depDict)

    def run(self, taskTypes: Iterable[Hashable], force: bool):
        taskGraph = TaskGraph({})
        for taskType in taskTypes:
            graph = self.__createGraph(taskType, force)
            taskGraph = taskGraph.merge(graph, {})
        
        taskGraph.printGraph()
        return runTaskGraph(taskGraph)

import json
from typing import List


# This is a required function that tells the backbone python module launcher where to find the component definition
def get_component_def():
    return ExampleWordCountBackboneComponent()


# This is a required function that tells the backbone python module launcher where to find the DoFn definition
def get_do_fn():
    return ExampleWordCountDoFn()


# This is an example backbone component definition. It is equivalent to a backbone transform component definition
# See: org.ohnlp.backbone.api.components.xlang.python.PythonBackbonePipelineComponent
# for interface specification
class ExampleWordCountBackboneComponent(object):
    def __init__(self):
        self.input_col = None
        self.output_col = None

    def init(self, configstr: str) -> None:
        if configstr is not None:
            config = json.loads(configstr)
            self.input_col = config.input_col
            self.output_col = config.output_col

    def toDoFnConfig(self) -> str:
        return json.dumps({
            "input_col": self.input_col,
            "output_col": self.output_col
        })

    def getInputTag(self) -> str:
        return "*"

    def getOutputTags(self) -> List[str]:
        return ['Word Counts']

    def calculateOutputSchema(self, jsonifiedInputSchemas: dict) -> dict:
        schema = json.loads(jsonifiedInputSchemas.get('*'))
        schema[self.output_col] = "INT32"
        return {
            'Word Counts': json.dumps(schema)
        }

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonBackbonePipelineComponent"]


# This is an example do function. It is what actually defines what gets done per record.
# Please refer to documentation on org.ohnlp.backbone.api.components.xlang.python.PythonOneToOneTransformDoFn
# for function definitions
class ExampleWordCountDoFn(object):
    def __init__(self):
        self.output_col = None
        self.input_col = None

    def initFromDriver(self, configJsonStr: str) -> None:
        config = json.loads(configJsonStr)
        self.input_col = config.input_col
        self.output_col = config.output_col

    def onBundleStart(self) -> None:
        pass

    def onBundleEnd(self) -> None:
        pass

    def apply(self, inputRow: str) -> List[str]:
        input = json.loads(inputRow)
        input[self.output_col] = len(input[self.input_col])
        return [json.dumps(input)]

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonOneToOneTransformDoFn"]


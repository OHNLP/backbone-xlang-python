import json
from typing import List

from ohnlp.toolkit.backbone.api import BackboneComponent, BackboneComponentOneToOneDoFn, BackboneComponentDefinition, \
    Row, Schema


# This defines the component and tells the python bridge where to find/how to instantiate the component
# itself and associated (Apache-Beam equivalent) DoFn
class WordCountComponentDefinition(BackboneComponentDefinition):
    def get_component_def(self):
        return ExampleWordCountBackboneComponent()

    def get_do_fn(self):
        return ExampleWordCountDoFn()


# This is an example backbone component definition. It is equivalent to a backbone transform component definition
# See: org.ohnlp.backbone.api.components.xlang.python.PythonBackbonePipelineComponent
# for interface specification
class ExampleWordCountBackboneComponent(BackboneComponent):
    def __init__(self):
        super().__init__()
        self.input_col = None
        self.output_col = None

    def init(self, configstr: str) -> None:
        if configstr is not None:
            config = json.loads(configstr)
            self.input_col = config.input_col
            self.output_col = config.output_col

    def to_do_fn_config(self) -> str:
        return json.dumps({
            "input_col": self.input_col,
            "output_col": self.output_col
        })

    def get_input_tag(self) -> str:
        return "*"

    def get_output_tags(self) -> List[str]:
        return ['Word Counts']

    def calculate_output_schema(self, input_schema: Schema) -> dict[str, Schema]:
        # TODO
        input_schema[self.output_col] = "INT32"
        return {
            'Word Counts': input_schema
        }


# This is an example do function. It is what actually defines what gets done per record.
# Please refer to documentation on org.ohnlp.backbone.api.components.xlang.python.PythonOneToOneTransformDoFn
# for function definitions
class ExampleWordCountDoFn(BackboneComponentOneToOneDoFn):
    def __init__(self):
        super().__init__()
        self.output_col = None
        self.input_col = None

    def init_from_driver(self, configJsonStr: str) -> None:
        config = json.loads(configJsonStr)
        self.input_col = config.input_col
        self.output_col = config.output_col

    def on_bundle_start(self) -> None:
        pass

    def on_bundle_end(self) -> None:
        pass

    def apply(self, input_row: Row) -> List[Row]:
        pass  # TODO

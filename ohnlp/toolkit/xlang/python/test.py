from typing import List, Union, Dict

from ohnlp.toolkit.xlang.python.api import Component, ComponentDescription, Schema, ConfigurationProperty, TypeName, \
    TypeCollection, OneToOneTransform, PartitionedRowCollection


@ComponentDescription(
    name= "Test Component",
    desc= "Test Component Desc",
    config_fields= {
        "test_field_1": ConfigurationProperty(
            "test.out.1",
            "Test Field 1",
            TypeName.STRING
        ),
        "test_field_2": ConfigurationProperty(
            "test.out.2",
            "Test Field 2",
            TypeCollection.of_collection(TypeName.STRING)
        ),
    }
)
class TestComponent(OneToOneTransform):
    def get_input_tag(self) -> str:
        pass

    def get_output_tag(self) -> str:
        pass

    def expand_coll(self, input_val: PartitionedRowCollection) -> PartitionedRowCollection:
        pass

    def get_required_columns(self, input_tag: str) -> Union[Schema, None]:
        pass

    def calculate_output_schema(self, input_schemas: Dict[str, Schema]) -> Dict[str, Schema]:
        pass

    def init(self):
        pass

    def teardown(self):
        pass

    def to_java(self):
        pass

    test_field_1: str = ''
    test_field_2: List[str] = []




if __name__ == "__main__":
    component = TestComponent()
    component.inject_config({
        "test": {
            "out": {
                "1": "asdf",
                "2": "asdf2"
            }
        }
    })
    val = 2

from typing import List, Union, Dict

from ohnlp.toolkit.xlang.python.api import BackboneComponent, ComponentDescription, Schema, ConfigurationProperty, TypeName, \
    TypeCollection


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
class TestComponent(BackboneComponent):

    test_field_1: str = ''
    test_field_2: List[str] = []

    def init(self, configstr: Union[str, None]) -> None:
        pass

    def to_do_fn_config(self) -> str:
        return ""

    def get_input_tag(self) -> str:
        return ""

    def get_output_tags(self) -> List[str]:
        return [""]

    def calculate_output_schema(self, input_schema: Dict[str, Schema]) -> Dict[str, Schema]:
        return {}


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

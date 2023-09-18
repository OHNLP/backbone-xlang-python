from abc import abstractmethod, ABC
from typing import Any, Generic, TypeVar, Union, Dict
from uuid import UUID

from py4j.java_collections import JavaMap
from py4j.java_gateway import JavaGateway

from ohnlp.toolkit.backbone.api import TypeName, TypeCollection

# Configuration Types
class InputColumn(object):
    sourceTag: str = None
    sourceColumnName: str = None

class TypeCollection(object):
    key_type: Union[TypeName, object, TypeCollection, None] = None
    value_type: Union[TypeName, object, TypeCollection]

    @staticmethod
    def of_collection(element_type: Union[TypeName, TypeCollection, object]) -> TypeCollection:
        ret = TypeCollection()
        ret.key_type = None
        ret.value_type = element_type
        return ret

    @staticmethod
    def of_map(key_type: Union[TypeName, TypeCollection, object], value_type: Union[TypeName, TypeCollection, object]) -> TypeCollection:
        ret = TypeCollection()
        ret.key_type = key_type
        ret.value_type = value_type
        return ret


class ConfigurationProperty(object):
    def __init__(self, path: str, desc: str, type_desc: Union[TypeName, object, TypeCollection], default = None):
        self._path: str = path
        self._desc: str = desc
        self._type: Union[TypeName, object, TypeCollection] = type_desc
        self._value = default

    @property
    def path(self):
        return self._path


# Class/Method Decorators for Reflection/Dynamic Scanning and Configuration Injection
class ComponentDescription(object):
    def __init__(self, name: str, desc: str, config_fields: Dict[str, ConfigurationProperty]):
        self._name = name
        self._desc = desc
        self._config_fields = config_fields

    def __call__(self, component):
        def inject_config(_, json_config):
            for key in self._config_fields:
                prop = self._config_fields[key]
                path: List[str] = prop.path.split(".")
                curr_val = json_config
                for item in path:
                    if item in curr_val and curr_val[item] is not None:
                        curr_val = curr_val[item]
                    else:
                        curr_val = None
                        break
                if curr_val is not None:
                    setattr(self._component, key, curr_val)


        self._component = component
        component._component_name = self._name
        component._component_desc = self._desc
        component._config_fields = self._config_fields
        component.inject_config = inject_config
        # Check for existence of config fields
        for key in self._config_fields:
            if not hasattr(component, key):
                print(f"Component {self._name} declares injectable config field {key} which does not exist within the class. This is not recommended due to potential of typos during implementation/maintenance")


        return component

class FunctionIdentifier(object):
    def __init__(self, transform_uid: UUID):
        self._uid = transform_uid

    def __call__(self, function):
        function.ohnlptk_component_uid = self._uid
        return function


# Data Structure Types
class WrappedJavaObject(ABC):

    _java: Any = None

    def init_java(self, jvm, java_obj):
        self._jvm = jvm
        self._java = java_obj

    @abstractmethod
    def to_java(self, _jvm):
        pass
class Row(WrappedJavaObject):



    def to_java(self, _jvm):
        pass

class Schema(WrappedJavaObject):

    def to_java(self, _jvm):
        pass


# Partitioned Collection Types
class PartitionedRowCollection(object):
    def __init__(self, gateway: JavaGateway, java_component, java_pcoll):
        self._gateway = gateway
        self._java_component = java_component
        self._internal_pcoll = java_pcoll

    def init_java(self, java_component):
        self._java_component = java_component

    def apply(self, desc: str, func: ) -> PartitionedRowCollection:
        # First, create a python callback dofn
        dofn = self._java_component.create_python_collection_transform(file_name_containing_do_fn, do_fn_class_name, output_schema.to_java())
        # Now directly apply on the internal pcoll, and wrap with a new PartitionedRowCollection
        return PartitionedRowCollection(
            self._gateway,
            self._java_component,
            self._internal_pcoll.apply(desc, dofn)
        )

    def to_java(self):
        return self._internal_pcoll

class PartitionedRowCollectionTuple(object):

    def __init__(self, gateway: JavaGateway, java_component = None, java_tuple = None):
        self._gateway: JavaGateway = gateway
        self._java_component = java_component
        self._internal: dict[str, PartitionedRowCollection] = {}
        if java_tuple is not None:
            as_map: JavaMap[str, object] = java_tuple.getAll()
            for key in as_map.keys():
                self._internal[key] = PartitionedRowCollection(self._gateway, self._java_component, as_map.get(key))

    def init_java(self, driver_component):
        self._java_component = driver_component
        # Touch all children within the _internal dict to update java components
        for child in self._internal.values():
            child.init_java(self._java_component)

    def to_java(self):
        java_tuple = self._java_component.create_new_tuple()
        for key in self._internal:
            java_tuple = self._gateway.get_method(java_tuple, "and")(key, self._internal[key].to_java())
        return java_tuple


# Component and Transform Types
IN_TYPE = TypeVar("IN_TYPE")
OUT_TYPE = TypeVar("OUT_TYPE")
class BackboneComponent(Generic[IN_TYPE, OUT_TYPE], WrappedJavaObject):

    def to_java(self, _jvm):
        pass

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def expand(self, input_val: IN_TYPE) -> OUT_TYPE:
        pass

    @abstractmethod
    def teardown(self):
        pass

class BackboneTransformComponent(BackboneComponent[PartitionedRowCollectionTuple, PartitionedRowCollectionTuple], ABC):
    @abstractmethod
    def get_required_columns(self, input_tag: str) -> Union[Schema, None]:
        pass

class TransformFunction(Generic[IN_TYPE, OUT_TYPE], ABC):

    @abstractmethod
    def expand(self, input: IN_TYPE) -> OUT_TYPE:
        pass


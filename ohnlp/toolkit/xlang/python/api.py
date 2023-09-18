from __future__ import annotations

import json
import uuid
from abc import abstractmethod, ABC
from typing import Any, Generic, TypeVar, Union, Dict, List, Iterable, overload, Type, Optional
from uuid import UUID

from py4j.java_collections import JavaMap, ListConverter
from py4j.java_gateway import JavaGateway, JVMView, JavaObject

from ohnlp.toolkit.backbone.api import TypeName

I = TypeVar("I")
O = TypeVar("O")


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
    def of_map(key_type: Union[TypeName, TypeCollection, object],
               value_type: Union[TypeName, TypeCollection, object]) -> TypeCollection:
        ret = TypeCollection()
        ret.key_type = key_type
        ret.value_type = value_type
        return ret


class ConfigurationProperty(object):
    def __init__(self, path: str, desc: str, type_desc: Union[TypeName, object, TypeCollection], default=None):
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
            for config_field_name in self._config_fields:
                prop = self._config_fields[config_field_name]
                path: List[str] = prop.path.split(".")
                curr_val = json_config
                for item in path:
                    if item in curr_val and curr_val[item] is not None:
                        curr_val = curr_val[item]
                    else:
                        curr_val = None
                        break
                if curr_val is not None:
                    setattr(self._component, config_field_name, curr_val)

        self._component = component
        component._component_name = self._name
        component._component_desc = self._desc
        component._config_fields = self._config_fields
        component.inject_config = inject_config
        # Check for existence of config fields
        for key in self._config_fields:
            if not hasattr(component, key):
                print(f"Component {self._name} declares injectable config field {key} which does not exist within the "
                      f"class. This is not recommended due to potential of typos during implementation/maintenance")

        return component


class FunctionIdentifier(object):
    def __init__(self, transform_uid: UUID):
        self._uid = transform_uid

    def __call__(self, function):
        function.toolkit_component_uid = self._uid
        return function


# Data Structure Types
class WrappedJavaObject(ABC):
    _gateway: JavaGateway = None
    _jvm: JVMView = None
    _java_obj: Any = None
    _toolkit_context: Any = None

    def init_java(self, gateway, java_obj, toolkit_context):
        self._gateway = gateway
        self._jvm = gateway.jvm
        self._java_obj = java_obj
        self._toolkit_context = toolkit_context

    @abstractmethod
    def to_java(self):
        pass


class Row(WrappedJavaObject):
    def to_java(self):
        pass


class Schema(WrappedJavaObject):
    # TODO implement init_java to create local clone

    def to_java(self):
        pass


# Partitioned Collection Types
# class PartitionedCollection(Generic[IN_TYPE, OUT_TYPE]): TODO consider implementing this
class PartitionedRowCollection(WrappedJavaObject):

    def get_schema(self) -> Schema:
        ret = Schema()
        ret.init_java(self._jvm, self._java_obj.getSchema(), self._toolkit_context)
        return ret

    def apply(self, desc: str,
              func: RowTransformFunction,
              output_schema: Schema,
              config: Dict) -> PartitionedRowCollection:
        # First, create a python callback dofn
        # - Retrieve the component uid
        if func.toolkit_component_uid is None:
            raise AssertionError(f"Transform function for {desc} was not initialized with the FunctionIdentifier "
                                 f"decorator!")
        # - Create relevant PCollection<Row> -> PCollection<Row> transform on the java side and pass
        # - the wrapped reference here
        java_transform_func = self._toolkit_context.create_python_collection_transform(
            str(func.toolkit_component_uid),
            json.dumps(config),
            output_schema.to_java())
        # Now directly apply on the internal pcoll, and wrap with a new PartitionedRowCollection
        result_pcoll = PartitionedRowCollection()
        result_pcoll.init_java(self._jvm, self._java_obj.apply(desc, java_transform_func), self._toolkit_context)
        return result_pcoll

    def to_java(self):
        return self._java_obj


class PartitionedRowCollectionTuple(WrappedJavaObject):

    def __init__(self):
        self._internal: dict[str, PartitionedRowCollection] = {}

    def init_java(self, gateway, java_obj, toolkit_context):
        super().init_java(gateway, java_obj, toolkit_context)
        if self._java_obj is not None:
            as_map: JavaMap[str, object] = self._java_obj.getAll()
            for key in as_map.keys():
                self._internal[key] = PartitionedRowCollection()
                self._internal[key].init_java(self._jvm, as_map.get(key), self._toolkit_context)

    def to_java(self):
        java_tuple = self._toolkit_context.create_new_tuple()
        for key in self._internal:
            java_tuple = self._gateway.get_method(java_tuple, "and")(key, self._internal[key].to_java())
        return java_tuple

    def get_keys(self):
        return self._internal.keys()

    def get(self, key: str):
        return self._internal[key]

    def add(self, key: str, collection: PartitionedRowCollection):
        self._internal[key] = collection


# Component and Transform Types
class RowTransformFunction(ABC):
    toolkit_component_uid: UUID = None

    @abstractmethod
    def init_from_driver(self):
        pass

    @abstractmethod
    def on_bundle_start(self):
        pass

    @abstractmethod
    def expand(self, input_value: PartitionedRowCollection) -> PartitionedRowCollection:
        pass

    @abstractmethod
    def on_bundle_end(self):
        pass

    @abstractmethod
    def on_teardown(self):
        pass


class Component(Generic[I, O], WrappedJavaObject):
    _name: str
    _desc: str
    _config_fields: Dict[str, ConfigurationProperty]

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def expand(self, input_val: I) -> O:
        pass

    @abstractmethod
    def teardown(self):
        pass

    def inject_config(self, config: Dict):
        pass  # Method overwritten by ComponentDescription decorator

    @property
    def name(self):
        return self._name


class Transform(Component[PartitionedRowCollectionTuple, PartitionedRowCollectionTuple], ABC):
    @abstractmethod
    def get_required_columns(self, input_tag: str) -> Union[Schema, None]:
        pass

    @abstractmethod
    def get_input_tags(self) -> List[str]:
        pass

    @abstractmethod
    def get_output_tags(self) -> List[str]:
        pass

    @abstractmethod
    def calculate_output_schema(self, input_schemas: Dict[str, Schema]) -> Dict[str, Schema]:
        pass

    def expand(self, input_val: PartitionedRowCollectionTuple) -> PartitionedRowCollectionTuple:
        return self.expand(input_val)


class OneToOneTransform(Transform, ABC):

    @abstractmethod
    def get_input_tag(self) -> str:
        pass

    @abstractmethod
    def get_output_tag(self) -> str:
        pass

    def get_input_tags(self) -> List[str]:
        return [self.get_input_tag()]

    def get_output_tags(self) -> List[str]:
        return [self.get_output_tag()]

    def expand(self, input_val: PartitionedRowCollectionTuple) -> PartitionedRowCollectionTuple:
        if len(input_val.get_keys()) != 1:
            raise ValueError(f"Expected single element input, got {len(input_val.get_keys())} instead")
        ret = PartitionedRowCollectionTuple()
        ret.add(self.get_output_tag(), self.expand_coll(input_val.get(input_val.get_keys()[0])))
        return ret

    @abstractmethod
    def expand_coll(self, input_val: PartitionedRowCollection) -> PartitionedRowCollection:
        pass


class ManyToOneTransform(Transform, ABC):

    @abstractmethod
    def get_output_tag(self) -> str:
        pass

    def get_output_tags(self) -> List[str]:
        return [self.get_output_tag()]

    def expand(self, input_val: PartitionedRowCollectionTuple) -> PartitionedRowCollectionTuple:
        ret = PartitionedRowCollectionTuple()
        ret.add(self.get_output_tag(), self.reduce(input_val))
        return ret

    @abstractmethod
    def reduce(self, input_val: PartitionedRowCollectionTuple) -> PartitionedRowCollection:
        pass


class OneToManyTransform(Transform, ABC):

    @abstractmethod
    def get_input_tag(self) -> str:
        pass

    def get_input_tags(self) -> List[str]:
        return [self.get_input_tag()]

    def expand(self, input_val: PartitionedRowCollectionTuple) -> PartitionedRowCollectionTuple:
        if len(input_val.get_keys()) != 1:
            raise ValueError(f"Expected single element input, got {len(input_val.get_keys())} instead")
        return self.expand_coll(input_val.get(input_val.get_keys()[0]))

    @abstractmethod
    def expand_coll(self, input_val: PartitionedRowCollection) -> PartitionedRowCollectionTuple:
        pass


# Top level Backbone Module Declaration

class ModuleDeclaration(object):
    def __init__(self, registered_components: List[Type[Transform]],
                 registered_functions: List[Type[RowTransformFunction]]):
        self._registered_components = registered_components
        self._registered_functions = registered_functions

    def __call__(self, module):
        module._registered_components = {}
        module._registered_functions = {}
        for component in self._registered_components:
            module._registered_components[component.name] = component
        for function in self._registered_functions:
            module._registered_functions[function.toolkit_component_uid] = function


class ToolkitModule(ABC):
    r"""
    Serves as an entry-point for python<->java communication.
    Modules should extend this class and decorate with the ModuleDeclaration decorator
    within their implementation, then point to said class within backbone_module.json
    """
    _registered_components: Dict[str, Type[Transform]]
    _registered_modules: Dict[str, Type[RowTransformFunction]]
    _active_components: Dict[str, Transform] = {}
    _calling_component: Any
    _toolkit_context: Any

    def __init__(self, gateway: JavaGateway):
        """Implementations should typically not produce their own gateway/this is injected by the module launcher

        :param gateway: The JavaGateway/py4j bridge that provides access to the underlying JVM
        """
        self._gateway = gateway

    def init_java(self, java_component, toolkit_context):
        self._calling_component = java_component
        self._toolkit_context = toolkit_context

    # Transform-related methods
    def register_transform(self, name: str, conf_json_str: str) -> str:
        """ Instantiates a new python transform instance, injects configuration values,
        and returns its UID for later reference.

        :param name:
        :param jvm_component:
        :param toolkit_context:
        :param conf_json_str:
        :return:
        """
        # We cannot directly pass the instance due to issues with memory referencing that is not part of the declared
        # interface becoming inaccessible outside the entry point rendering java interface implementation infeasible
        # TODO see if this can be fixed
        if name not in self._registered_components:
            raise NameError(f"Component {name} not found or is not registered via @ModuleDeclaration!")
        instance = self._registered_components[name]()
        instance.init_java(self._gateway, self._calling_component, self._toolkit_context)
        if conf_json_str is not None:
            instance.inject_config(json.loads(conf_json_str))
        instance_uid = str(uuid.uuid4())
        self._active_components[instance_uid.lower()] = instance
        return instance_uid

    def call_transform_init(self, component_uid: str):
        if component_uid.lower() not in self._active_components:
            raise NameError(f"Component {component_uid} called when it is not active/was already unregistered")
        self._active_components.get(component_uid.lower()).init()

    def call_transform_expand(self, component_uid: str, java_pcolltuple):
        if component_uid.lower() not in self._active_components:
            raise NameError(f"Component {component_uid} called when it is not active/was already unregistered")
        transform = self._active_components.get(component_uid.lower())
        python_tuple = PartitionedRowCollectionTuple()
        python_tuple.init_java(self._gateway, java_pcolltuple, self._toolkit_context)
        return transform.expand(python_tuple).to_java()

    def call_transform_get_inputs(self, component_uid: str):
        if component_uid.lower() not in self._active_components:
            raise NameError(f"Component {component_uid} called when it is not active/was already unregistered")
        transform = self._active_components.get(component_uid.lower())
        # noinspection PyProtectedMember
        return ListConverter().convert(transform.get_input_tags(), self._gateway._gateway_client)

    def call_transform_get_outputs(self, component_uid: str):
        if component_uid.lower() not in self._active_components:
            raise NameError(f"Component {component_uid} called when it is not active/was already unregistered")
        transform = self._active_components.get(component_uid.lower())
        # noinspection PyProtectedMember
        return ListConverter().convert(transform.get_output_tags(), self._gateway._gateway_client)

    def call_transform_get_required_columns(self, component_uid: str, tag: str):
        if component_uid.lower() not in self._active_components:
            raise NameError(f"Component {component_uid} called when it is not active/was already unregistered")
        transform = self._active_components.get(component_uid.lower())
        required_columns = transform.get_required_columns(tag)
        if required_columns is None:
            return None
        else:
            return required_columns.to_java()

    def call_transform_get_output_schema(self, component_uid: str, java_input_schemas):
        if component_uid.lower() not in self._active_components:
            raise NameError(f"Component {component_uid} called when it is not active/was already unregistered")
        transform = self._active_components.get(component_uid.lower())
        python_input_schemas: Dict[str, Schema] = {}
        for key in java_input_schemas:
            python_input_schemas[key] = Schema()
            python_input_schemas[key].init_java(self._gateway, java_input_schemas[key], self._toolkit_context)
        python_output_schemas: Dict[str, Schema] = transform.calculate_output_schema(python_input_schemas)
        java_output_schemas = self._gateway.jvm.java.util.HashMap()
        for key in python_output_schemas:
            java_output_schemas.put(key, python_output_schemas[key].to_java())
        return java_output_schemas

    def call_transform_teardown(self, component_uid: str):
        if component_uid.lower() not in self._active_components:
            raise NameError(f"Component {component_uid} called when it is not active/was already unregistered")
        transform = self._active_components.get(component_uid.lower())
        transform.teardown()

    class Java:
        implements = ["org.ohnlp.toolkit.xlang.python.PythonEntryPoint"]


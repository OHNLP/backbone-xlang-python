from __future__ import annotations

import json
import uuid
from abc import abstractmethod, ABC
from enum import Enum
from typing import Any, Generic, TypeVar, Union, Dict, List, Type, Optional
from uuid import UUID

from py4j.java_collections import JavaMap, ListConverter
from py4j.java_gateway import JavaGateway, JVMView, JavaObject, is_instance_of

from ohnlp.toolkit.backbone.api import TypeName

# Global Component/Function Registries
_registered_components: Dict[str, Type[Transform]] = {}
_registered_udfs: Dict[str, Type[UserDefinedPartitionMappingFunction]] = {}
_active_components: Dict[str, Transform] = {}
_active_udfs: Dict[str, UserDefinedPartitionMappingFunction] = {}
_gateway: JavaGateway


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

    def init_java(self, gateway, java_obj):
        self._gateway = gateway
        self._jvm = gateway.jvm
        self._java_obj = java_obj

    @abstractmethod
    def to_java(self):
        pass


class Row(WrappedJavaObject):

    @staticmethod
    def of_java(jvm_row):
        ret = Row()
        ret.init_java(_gateway, jvm_row)
        return ret

    @staticmethod
    def of(schema: Schema, values: List = None):
        if values is None:
            values = [None] * len(schema.get_fields())
        ret = Row()
        # noinspection PyProtectedMember
        jvm_row = _gateway.jvm.org.apache.beam.sdk.values.Row.withSchema(
            schema.to_java()
        ).addValues(
            ListConverter().convert(values, _gateway._gateway_client)
        ).build()
        ret.init_java(_gateway, jvm_row)
        return ret

    def get_field_index(self, field_name: str) -> Optional[int]:
        return self._java_obj.getSchema().indexOf(field_name)

    def get_schema(self) -> Schema:
        return Schema.of_java(self._java_obj.getSchema())

    def get_value(self, field_name: str) -> Optional[Any]:
        ret = self._java_obj.getValue(self._java_obj.getSchema().indexOf(field_name))
        if isinstance(ret, JavaObject):
            if is_instance_of(self._gateway, ret, "org.apache.beam.sdk.values.Row"):
                ret = Row.of_java(ret)
        return ret

    def set_value(self, field_name: str, value: Any):
        values = self._java_obj.getValues()
        field_idx = self._java_obj.getSchema().indexOf(field_name)
        if isinstance(value, JavaObject) and is_instance_of(self._gateway, value, "org.apache.beam.sdk.values.Row"):
            value = Row.of_java(value)
        values[field_idx] = value
        target_row = Row.of(self.get_schema(), values)
        # Target Row is a new Row instance/Rows are immutable in Java, so we need to replace the wrapped object
        # with the new instance instead
        self._java_obj = target_row._java_obj

    def to_java(self):
        return self._java_obj


class Schema(WrappedJavaObject):
    @staticmethod
    def of(fields: List[Field]):
        java_fields = map(lambda f: f.to_java(), fields)
        ret = Schema()
        # noinspection PyProtectedMember
        ret.init_java(_gateway, _gateway.jvm.org.apache.beam.sdk.schemas.Schema.of(
            ListConverter().convert(java_fields, _gateway._gateway_client)
        ))

    @staticmethod
    def of_java(java_schema) -> Schema:
        ret = Schema()
        ret.init_java(_gateway, java_schema)
        return ret

    def to_java(self):
        pass


class Field:
    _name: str
    _type: FieldType
    _nullable: bool

    @staticmethod
    def of(name: str, field_type: FieldType):
        ret = Field()
        ret._name = name
        ret._type = field_type
        ret._nullable = False
        return ret

    @staticmethod
    def of_nullable(name: str, field_type: FieldType):
        ret = Field.of(name, field_type)
        ret._nullable = True
        return ret

    def to_java(self):
        if not self._nullable:
            return _gateway.jvm.org.apache.beam.sdk.schemas.Schema.Field.of(self._name, self._type.to_java())
        else:
            return _gateway.jvm.org.apache.beam.sdk.schemas.Schema.Field.nullable(self._name, self._type.to_java())


class FieldType:
    _internal_type: TypeName
    _field_schema: Schema
    _value_type: FieldType

    @staticmethod
    def of(type_name: TypeName) -> FieldType:
        ret = FieldType()
        ret._internal_type = type_name
        return ret

    @staticmethod
    def of_row(schema: Schema) -> FieldType:
        ret = FieldType()
        ret._internal_type = TypeName.ROW
        ret._field_schema = schema
        return ret

    @staticmethod
    def of_arr(element_type: FieldType) -> FieldType:
        ret = FieldType()
        ret._internal_type = TypeName.ARRAY
        ret._value_type = element_type
        return ret

    def to_java(self):
        if self._internal_type.value is not None:
            return _gateway.jvm.org.apache.beam.sdk.schemas.Schema.FieldType.of(
                self._internal_type.value
            )
        elif self._internal_type.name == 'ROW':
            if self._field_schema is None:
                raise ValueError("Row FieldTypes should be initialized with FieldType#of_row(), not FieldType#of()")
            else:
                return _gateway.jvm.org.apache.beam.sdk.schemas.Schema.FieldType.row(self._field_schema.to_java())
        elif self._internal_type.name == 'ARRAY':
            if self._value_type is None:
                raise ValueError("Array FieldTypes should be initialized with FieldType#of_arr(), not FieldType#of()")
            else:
                return _gateway.jvm.org.apache.beam.sdk.schemas.Schema.FieldType.array(
                    self._value_type.to_java(), True)


class TypeName(Enum):
    STRING = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.STRING,
    BYTE = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.BYTE,
    BYTES = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.BYTES,
    INT16 = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.INT16,
    INT32 = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.INT32,
    INT64 = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.INT64,
    FLOAT = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.FLOAT,
    DOUBLE = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.DOUBLE,
    DECIMAL = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.DECIMAL,
    BOOLEAN = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.BOOLEAN,
    DATETIME = _gateway.jvm.org.apache.beam.sdk.schemas.Schema.TypeName.DATETIME,
    ROW = None,
    ARRAY = None


class SchemaField(object):
    def __init__(self, name: str, field_meta: FieldType):
        self._name: str = name
        self._field_type: FieldType = field_meta

    def get_name(self) -> str:
        return self._name

    def get_field_type(self) -> FieldType:
        return self._field_type


# Partitioned Collection Types
PCOLL_TYPE = TypeVar("PCOLL_TYPE")


class PartitionedCollection(Generic[PCOLL_TYPE], WrappedJavaObject):
    def __init__(self, creating_transform):
        self._transform_obj = creating_transform

    def apply(self, desc: str,
              func: UserDefinedPartitionMappingFunction[PCOLL_TYPE, UDF_OUT_TYPE],
              config: Dict) -> PartitionedCollection[UDF_OUT_TYPE]:
        # First, create a python callback dofn
        # - Retrieve the component uid
        if func.toolkit_component_uid is None:
            raise AssertionError(f"Transform function for {desc} was not initialized with the FunctionIdentifier "
                                 f"decorator!")
        # - Create relevant PCollection<PCOLL_TYPE> -> PCollection<UDF_OUT_TYPE> transform on the java side and pass
        # - the wrapped reference here
        java_transform_func = self._transform_obj.callUDF(str(func.toolkit_component_uid), json.dumps(config))
        # Now directly apply on the internal pcoll, and wrap with a new PartitionedRowCollection
        result_pcoll: PartitionedCollection[UDF_OUT_TYPE] = PartitionedCollection[UDF_OUT_TYPE](self._transform_obj)
        result_pcoll.init_java(self._jvm, self._java_obj.apply(desc, java_transform_func))
        return result_pcoll

    def get_schema(self) -> Schema:
        ret = Schema()
        ret.init_java(self._jvm, self._java_obj.getSchema())
        return ret

    def set_encoder(self, encoder):
        self._java_obj.setCoder(encoder.to_java_coder())

    def to_java(self):
        return self._java_obj


class PartitionedRowCollectionTuple(WrappedJavaObject):

    def __init__(self, creating_transform):
        self._internal: dict[str, PartitionedCollection[Row]] = {}
        self._transform_obj = creating_transform

    def init_java(self, gateway, java_obj):
        super().init_java(gateway, java_obj)
        if self._java_obj is not None:
            as_map: JavaMap[str, object] = self._java_obj.getAll()
            for key in as_map.keys():
                self._internal[key] = PartitionedCollection[Row](self._transform_obj)
                self._internal[key].init_java(self._jvm, as_map.get(key))

    def to_java(self):
        java_tuple = self._gateway.jvm.org.apache.beam.sdk.values.PCollectionRowTuple.empty(
            self._java_obj.getPipeline())
        for key in self._internal:
            java_tuple = self._gateway.get_method(java_tuple, "and")(key, self._internal[key].to_java())
        return java_tuple

    def get_keys(self):
        return self._internal.keys()

    def get(self, key: str):
        return self._internal[key]

    def add(self, key: str, collection: PartitionedCollection[Row]):
        self._internal[key] = collection


# Component and Transform Types
class OutputCollector(WrappedJavaObject):

    def output(self, obj: Any):
        if isinstance(obj, WrappedJavaObject):
            self._java_obj.output(obj.to_java())
        else:
            self._java_obj.output(obj)  # TODO type-check this/ensure that it is autoconvertible

    def output_tagged(self, tag: str, obj: Any):
        jvm_tag = self._gateway.jvm.org.apache.beam.sdk.values.TupleTag(tag)
        if isinstance(obj, WrappedJavaObject):
            self._java_obj.output(jvm_tag, obj.to_java())
        else:
            self._java_obj.output(jvm_tag, obj)  # TODO type-check this/ensure that it is autoconvertible

    def to_java(self):
        return self._java_obj


UDF_IN_TYPE = TypeVar("UDF_IN_TYPE")
UDF_OUT_TYPE = TypeVar("UDF_OUT_TYPE")


class UserDefinedPartitionMappingFunction(Generic[UDF_IN_TYPE, UDF_OUT_TYPE], ABC):
    toolkit_component_uid: UUID = None

    @abstractmethod
    def init_from_driver(self, json_config: Optional[Dict]) -> None:
        pass

    @abstractmethod
    def on_bundle_start(self) -> None:
        pass

    @abstractmethod
    def apply(self, out: OutputCollector, input_value: Any) -> None:
        pass

    @abstractmethod
    def on_bundle_finish(self, out: OutputCollector) -> None:
        pass

    @abstractmethod
    def on_teardown(self) -> None:
        pass


COMPONENT_INPUT_T = TypeVar("COMPONENT_INPUT_T")
COMPONENT_OUTPUT_T = TypeVar("COMPONENT_OUTPUT_T")


class Component(Generic[COMPONENT_INPUT_T, COMPONENT_OUTPUT_T], WrappedJavaObject):
    _name: str
    _desc: str
    _config_fields: Dict[str, ConfigurationProperty]

    @abstractmethod
    def init(self):
        pass

    @abstractmethod
    def expand(self, input_val: COMPONENT_INPUT_T) -> COMPONENT_OUTPUT_T:
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
        ret = PartitionedRowCollectionTuple(self._java_obj)
        ret.add(self.get_output_tag(), self.expand_coll(input_val.get(input_val.get_keys()[0])))
        return ret

    @abstractmethod
    def expand_coll(self, input_val: PartitionedCollection[Row]) -> PartitionedCollection[Row]:
        pass


class ManyToOneTransform(Transform, ABC):

    @abstractmethod
    def get_output_tag(self) -> str:
        pass

    def get_output_tags(self) -> List[str]:
        return [self.get_output_tag()]

    def expand(self, input_val: PartitionedRowCollectionTuple) -> PartitionedRowCollectionTuple:
        ret = PartitionedRowCollectionTuple(self._java_obj)
        ret.add(self.get_output_tag(), self.reduce(input_val))
        return ret

    @abstractmethod
    def reduce(self, input_val: PartitionedRowCollectionTuple) -> PartitionedCollection[Row]:
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
    def expand_coll(self, input_val: PartitionedCollection[Row]) -> PartitionedRowCollectionTuple:
        pass


# Top level Backbone Module Declaration
class ModuleDeclaration(object):
    def __init__(self, registered_components: List[Type[Transform]],
                 registered_functions: List[Type[UserDefinedPartitionMappingFunction]]):
        self._registered_components = registered_components
        self._registered_functions = registered_functions

    def __call__(self, module):
        for component in self._registered_components:
            _registered_components[component().name] = component
        for function in self._registered_functions:
            _registered_udfs[str(function.toolkit_component_uid)] = function


class ToolkitModule(ABC):
    r"""
    Serves as an entry-point for python<->java communication.
    Modules should extend this class and decorate with the ModuleDeclaration decorator
    within their implementation, then point to said class within backbone_module.json
    """
    _calling_component: Any
    _gateway: JavaGateway

    def python_init(self, gateway: JavaGateway):
        """Implementations should typically not produce their own gateway/this is injected by the module launcher

        :param gateway: The JavaGateway/py4j bridge that provides access to the underlying JVM
        """
        self._gateway = gateway
        global _gateway
        _gateway = gateway  # Set global for ease of access in row/schema static creation

    def java_init(self, java_component):
        self._calling_component = java_component

    @staticmethod
    def check_and_get_active_component(component_uid: str) -> Transform:
        if component_uid.lower() not in _active_components:
            raise NameError(f"Component {component_uid} called when it is not active/was already unregistered")
        return _active_components.get(component_uid.lower())

    @staticmethod
    def check_and_get_active_function(udf_uid: str) -> UserDefinedPartitionMappingFunction:
        if udf_uid.lower() not in _active_udfs:
            raise NameError(f"Function {udf_uid} called when it is not active/was already unregistered")
        return _active_udfs.get(udf_uid.lower())

    # Transform-related methods
    def register_transform_instance(self, name: str) -> str:
        """ Instantiates a new python transform instance, injects configuration values,
        and returns its UID for later reference.

        :param name:
        :return:
        """
        # We cannot directly pass the instance due to issues with memory referencing that is not part of the declared
        # interface becoming inaccessible outside the entry point rendering java interface implementation infeasible
        # TODO see if this can be fixed
        if name not in _registered_components:
            raise NameError(f"Component {name} not found or is not registered via @ModuleDeclaration!")
        instance = _registered_components[name]()
        instance.init_java(self._gateway, self._calling_component)

        instance_uid = str(uuid.uuid4())
        _active_components[instance_uid.lower()] = instance
        return instance_uid

    def call_transform_init(self, component_uid: str, conf_json_str: str):
        transform = self.check_and_get_active_component(component_uid)
        if conf_json_str is not None:
            transform.inject_config(json.loads(conf_json_str))
        transform.init()

    def call_transform_expand(self, component_uid: str, java_pcolltuple):
        transform = self.check_and_get_active_component(component_uid)
        python_tuple = PartitionedRowCollectionTuple(self._calling_component)
        python_tuple.init_java(self._gateway, java_pcolltuple)
        return transform.expand(python_tuple).to_java()

    def call_transform_get_inputs(self, component_uid: str):
        transform = self.check_and_get_active_component(component_uid)
        # noinspection PyProtectedMember
        return ListConverter().convert(transform.get_input_tags(), self._gateway._gateway_client)

    def call_transform_get_outputs(self, component_uid: str):
        transform = self.check_and_get_active_component(component_uid)
        # noinspection PyProtectedMember
        return ListConverter().convert(transform.get_output_tags(), self._gateway._gateway_client)

    def call_transform_get_required_columns(self, component_uid: str, tag: str):
        transform = self.check_and_get_active_component(component_uid)
        required_columns = transform.get_required_columns(tag)
        if required_columns is None:
            return None
        else:
            return required_columns.to_java()

    def call_transform_get_output_schema(self, component_uid: str, java_input_schemas):
        transform = self.check_and_get_active_component(component_uid)
        python_input_schemas: Dict[str, Schema] = {}
        for key in java_input_schemas:
            python_input_schemas[key] = Schema()
            python_input_schemas[key].init_java(self._gateway, java_input_schemas[key])
        python_output_schemas: Dict[str, Schema] = transform.calculate_output_schema(python_input_schemas)
        java_output_schemas = self._gateway.jvm.java.util.HashMap()
        for key in python_output_schemas:
            java_output_schemas.put(key, python_output_schemas[key].to_java())
        return java_output_schemas

    def call_transform_teardown(self, component_uid: str):
        transform = self.check_and_get_active_component(component_uid)
        transform.teardown()
        _active_components.pop(component_uid)

    # User-Defined Functions
    def register_udf(self, udf_uid: str) -> str:
        if udf_uid not in _registered_udfs:
            raise NameError(f"UDF {udf_uid} not found or is not registered via @ModuleDeclaration!")
        instance = _registered_udfs[udf_uid]()
        # TODO do we need to init from java?
        instance_uid = str(uuid.uuid4())
        _active_udfs[instance_uid.lower()] = instance
        return instance_uid

    def call_udf_on_init(self, udf_uid: str, conf_json_str: str):
        function = self.check_and_get_active_function(udf_uid)
        if conf_json_str is not None:
            function.init_from_driver(json.loads(conf_json_str))
        else:
            function.init_from_driver(None)

    def call_udf_on_bundle_start(self, udf_uid: str):
        function = self.check_and_get_active_function(udf_uid)
        function.on_bundle_start()

    def call_udf_apply(self, udf_uid: str, element, processcontext):
        function = self.check_and_get_active_function(udf_uid)
        output_context = OutputCollector()
        output_context.init_java(self._gateway, processcontext)
        element_to_process = element
        if isinstance(element, JavaObject):
            if is_instance_of(self._gateway, element, "org.apache.beam.sdk.values.Row"):
                element_to_process = Row.of_java(element)
            else:
                raise ValueError(f"Inconvertible object of type {element.getClass().getName()} supplied to UDF call")
        function.apply(output_context, element_to_process)  # TODO ensure convertible

    def call_udf_on_bundle_finish(self, udf_uid: str, processcontext):
        function = self.check_and_get_active_function(udf_uid)
        out = OutputCollector()
        out.init_java(self._gateway, processcontext)
        function.on_bundle_finish(out)

    def call_udf_on_teardown(self, udf_uid: str):
        function = self.check_and_get_active_function(udf_uid)
        function.on_teardown()

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonEntryPoint"]

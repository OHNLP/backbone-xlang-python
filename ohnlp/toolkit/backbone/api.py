from __future__ import annotations

import json
from abc import ABC, abstractmethod
from enum import Enum
from typing import List, Union, MutableMapping

from py4j.java_collections import ListConverter, MapConverter
from py4j.java_gateway import JavaGateway, JavaMember, JavaClass


class FieldType(object):
    def __init__(self, type_name: str, array_content_type: Union[FieldType, None] = None,
                 row_content_type: Union[Schema, None] = None):
        self._type_name: str = type_name
        self._array_content_type: FieldType = array_content_type
        self._content_obj_fields: Union[Schema, None] = row_content_type

    def get_type_name(self) -> str:
        return self._type_name

    def get_array_content_type(self) -> Union[None, FieldType]:
        return self._array_content_type

    def get_content_obj_fields(self) -> Union[Schema, None]:
        return self._content_obj_fields

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonSchema$PythonFieldType"]


class TypeName(Enum):
    STRING = "STRING",
    BYTE = "BYTE",
    BYTES = "BYTES",
    INT16 = "INT16",
    INT32 = "INT32",
    INT64 = "INT64",
    FLOAT = "FLOAT",
    DOUBLE = "DOUBLE",
    DECIMAL = "DECIMAL",
    BOOLEAN = "BOOLEAN",
    DATETIME = "DATETIME",
    ROW = "ROW",
    ARRAY = "ARRAY"


class SchemaField(object):
    def __init__(self, name: str, field_meta: FieldType):
        self._name: str = name
        self._field_type: FieldType = field_meta

    def get_name(self) -> str:
        return self._name

    def get_field_type(self) -> FieldType:
        return self._field_type

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonSchema$PythonSchemaField"]


class Schema(object):
    def __init__(self, fields: List[SchemaField], gateway: JavaGateway):
        self._gateway: JavaGateway = gateway
        self._fields: List[SchemaField] = fields
        self._fields_by_name: dict = {}
        for field in fields:
            self._fields_by_name[field.get_name()] = field

    def get_fields(self) -> Union[List[SchemaField], JavaClass]:
        return ListConverter().convert(self._fields, self._gateway._gateway_client)

    def get_field(self, name: str) -> SchemaField:
        return self._fields_by_name[name]

    def toString(self):
        ret: list[str] = []
        ret.append('Fields: ')
        for field in self._fields:
            ret.append('- ' + field.get_name() + ', ' + field.get_field_type().get_type_name())
        return '\r\n'.join(ret)

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonSchema"]


class Row(object):
    def __init__(self, schema: Schema, values: List[object]):
        self._schema: Schema = schema
        self._field_idx: dict[str, int] = {}
        for i in range(0, len(schema.get_fields())):
            self._field_idx[schema.get_fields()[i].get_name()] = i
        self._values: List[object] = values

    def get_schema(self) -> Schema:
        return self._schema

    def get_field_index(self, field_name: str) -> Union[int, None]:
        if self._field_idx[field_name] is not None:
            return self._field_idx[field_name]
        else:
            return None

    def get_value(self, field_name: str) -> Union[object, None]:
        index = self.get_field_index(field_name)
        return None if index is None else self._values[index]

    def get_values(self) -> Union[List[object], JavaClass]:
        return ListConverter().convert(self._values, self.get_schema()._gateway._gateway_client)

    def set_value(self, field_name: str, value: object):
        index = self.get_field_index(field_name)
        if index is not None:
            self._values[index] = value
        else:
            raise KeyError("Reference to non-existent field_name " + field_name + " in set_value")

    def toString(self):
        ret: list[str] = []
        ret.append('Schema: ' + self._schema.toString())
        ret.append('Values: ')
        for value in self._values:
            ret.append('- ' + str(value))
        return '\r\n'.join(ret)



    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonRow"]


class TaggedRow(object):
    def __init__(self, tag: str, row: Row):
        self._tag: str = tag
        self._row: Row = row

    def get_tag(self) -> str:
        return self._tag

    def get_row(self) -> Row:
        return self._row

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonTaggedRow"]


class BridgedInterfaceWithConvertableDataTypes(object):

    def __init__(self):
        self.gateway: Union[JavaGateway, None] = None

    def python_init(self, gateway: JavaGateway):
        self.gateway = gateway

    def jsonified_python_schema_from_json_string(self, json_schema: str) -> str:
        return self.json_string_from_python_schema(self.python_schema_from_json_string(json_schema))

    def python_schema_from_json_string(self, json_schema: str):
        schema_def: dict = json.loads(json_schema)
        return self.parse_schema_from_json(schema_def)

    def parse_schema_from_json(self, schema_def: dict) -> Schema:
        schema_fields: List[SchemaField] = []
        for field_name in schema_def:
            schema_fields.append(
                SchemaField(field_name, self.parse_schema_field_type_from_json(schema_def[field_name])))
        return Schema(schema_fields, self.gateway)

    def parse_schema_field_type_from_json(self, val) -> FieldType:
        if type(val) == str:
            return FieldType(val)
        elif type(val) == dict:
            return FieldType("ROW", row_content_type=self.parse_schema_from_json(val))
        else:
            return FieldType("ARRAY", array_content_type=self.parse_schema_field_type_from_json(val[0]))

    def python_row_from_json_string(self, json_row: str):
        data: dict = json.loads(json_row)
        schema: Schema = self.parse_schema_from_json(data['schema'])
        raw_row: dict = data['contents']
        return self.parse_row_from_json(schema, raw_row)

    def jsonified_python_row_from_json_string(self, json_row: str) -> str:
        return self.json_string_from_python_row(self.python_row_from_json_string(json_row))

    def parse_row_from_json(self, schema: Schema, data: dict) -> Row:
        values: List[object] = []
        for field in schema.get_fields():
            values.append(self.parse_field_value_from_json(field.get_field_type(), data[field.get_name()]))

        return Row(schema, values)

    def parse_field_value_from_json(self, content_type: FieldType, val) -> Union[object, None]:
        if val is None:
            return None
        else:
            if content_type.get_type_name() == "ROW":
                return self.parse_row_from_json(content_type.get_content_obj_fields(), val)
            elif content_type.get_type_name() == "ARRAY":
                child_type: FieldType = content_type.get_array_content_type()
                sub_values: List[object] = []
                for entry in val:
                    sub_values.append(self.parse_field_value_from_json(child_type, entry))
                return sub_values
            else:
                return val

    def json_string_from_python_schema(self, schema: Schema) -> str:
        return json.dumps(self.parse_schema_to_json(schema))

    def parse_schema_to_json(self, schema: Schema) -> dict:
        ret: dict = {}
        for field in schema.get_fields():
            ret[field.get_name()] = self.parse_schema_field_type_to_json(field.get_field_type())
        return ret

    def parse_schema_field_type_to_json(self, field_type: FieldType):
        if field_type.get_type_name() == "ROW":
            return self.parse_schema_to_json(field_type.get_content_obj_fields())
        elif field_type.get_type_name() == "ARRAY":
            return [self.parse_schema_field_type_to_json(field_type.get_array_content_type())]
        else:
            return field_type.get_type_name()

    def json_string_from_python_row(self, row: Row) -> str:
        schema_json = self.parse_schema_to_json(row.get_schema())
        contents = self.parse_row_to_json(row)
        return json.dumps({
            "schema": schema_json,
            "contents": contents
        })

    def parse_row_to_json(self, row: Row) -> dict:
        ret: dict = {}
        for field in row.get_schema().get_fields():
            ret[field.get_name()] = self.parse_row_field_value_to_json(field.get_field_type(), row.get_value(field.get_name()))
        return ret

    def parse_row_field_value_to_json(self, field_type: FieldType, data):
        if field_type.get_type_name() == "ROW":
            return self.parse_row_to_json(data)
        elif field_type.get_type_name() == "ARRAY":
            ret = []
            for element in data:
                ret.append(self.parse_row_field_value_to_json(field_type.get_array_content_type(), element))
            return ret
        else:
            return data


class BackboneComponentDefinition(ABC):

    @abstractmethod
    def get_component_def(self) -> BackboneComponent:
        pass

    @abstractmethod
    def get_do_fn(self) -> Union[BackboneComponentOneToOneDoFn, BackboneComponentOneToManyDoFn]:
        pass


class BackboneComponent(ABC, BridgedInterfaceWithConvertableDataTypes):

    @abstractmethod
    def init(self, configstr: Union[str, None]) -> None:
        pass

    @abstractmethod
    def to_do_fn_config(self) -> str:
        pass

    @abstractmethod
    def get_input_tag(self) -> str:
        pass

    def proxied_get_output_tags(self):
        return ListConverter().convert(self.get_output_tags(), self.gateway._gateway_client)

    @abstractmethod
    def get_output_tags(self) -> List[str]:
        pass

    def proxied_calculate_output_schema(self, input_schema: MutableMapping[str, Schema]):
        input_schema_converted: dict[str, Schema] = {}
        for key in input_schema.keys():
            input_schema_converted[key] = input_schema[key]
        return MapConverter().convert(self.calculate_output_schema(input_schema_converted), self.gateway._gateway_client)

    @abstractmethod
    def calculate_output_schema(self, input_schema: dict[str, Schema]) -> dict[str, Schema]:
        pass

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonBackbonePipelineComponent"]


class BackboneComponentOneToOneDoFn(ABC, BridgedInterfaceWithConvertableDataTypes):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def init_from_driver(self, config_json_str: Union[str, None]) -> None:
        pass

    @abstractmethod
    def on_bundle_start(self) -> None:
        pass

    @abstractmethod
    def on_bundle_end(self) -> None:
        pass

    def proxied_apply(self, input_row: Row):
        return ListConverter().convert(self.apply(input_row), self.gateway._gateway_client)

    @abstractmethod
    def apply(self, input_row: Row) -> List[Row]:
        pass

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonOneToOneTransformDoFn"]


class BackboneComponentOneToManyDoFn(ABC, BridgedInterfaceWithConvertableDataTypes):
    def __init__(self):
        super().__init__()

    @abstractmethod
    def init_from_driver(self, config_json_str: str) -> None:
        pass

    @abstractmethod
    def on_bundle_start(self) -> None:
        pass

    @abstractmethod
    def on_bundle_end(self) -> None:
        pass

    def proxied_apply(self, input_row: Row):
        return ListConverter().convert(self.apply(input_row), self.gateway._gateway_client)

    @abstractmethod
    def apply(self, input_row: Row) -> List[TaggedRow]:
        pass

    class Java:
        implements = ["org.ohnlp.backbone.api.components.xlang.python.PythonOneToManyTransformDoFn"]

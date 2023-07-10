import importlib
import json
import secrets
import string
import socket
from types import ModuleType

from py4j.clientserver import ClientServer, JavaParameters, PythonParameters

from ohnlp.toolkit.backbone.api import BackboneComponentDefinition


def find_free_port():
    sock = socket.socket()
    sock.bind(('', 0))
    return sock.getsockname()[1]


def launch_bridge(entrypoint: str, class_name: str, init_type: str):
    first_init: bool = init_type == 'component'

    # Import the backbone module to be used
    module: ModuleType = importlib.import_module(entrypoint)
    cls = getattr(module, class_name)
    entry_class: BackboneComponentDefinition = cls()

    # Generate an authentication token for this session
    auth_token = ''.join(secrets.choice(string.ascii_uppercase + string.digits)
                         for i in range(16))

    # Get appropriate entry point
    if first_init:
        entry_point = entry_class.get_component_def()
    else:
        entry_point = entry_class.get_do_fn()

    # Find available ports
    java_port = find_free_port()
    python_port = find_free_port()

    # Bootup python endpoint
    gateway = ClientServer(
        java_parameters=JavaParameters(port=java_port, auth_token=auth_token, auto_convert=True, auto_field=True),
        python_parameters=PythonParameters(port=python_port, auth_token=auth_token),
        python_server_entry_point=entry_point,
    )

    entry_point.python_init(gateway)

    java_port: int = gateway.java_parameters.port
    python_port: int = gateway.python_parameters.port

    # Write vars out to JSON
    with open('python_bridge_meta.json', 'w') as f:
        json.dump({
            'token': auth_token,
            'java_port': java_port,
            'python_port': python_port
        }, f)

    # Create monitor file used by java process to indicate gateway init complete
    with open('python_bridge_meta.done', 'w') as f:
        f.writelines('done')

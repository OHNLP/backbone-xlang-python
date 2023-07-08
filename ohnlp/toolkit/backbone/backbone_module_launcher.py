import importlib
import json
import secrets
import string
from types import ModuleType

from py4j.clientserver import ClientServer, JavaParameters, PythonParameters

from ohnlp.toolkit.backbone.api import BackboneComponentDefinition


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
    # Bootup python endpoint
    gateway = ClientServer(
        java_parameters=JavaParameters(auth_token=auth_token),
        python_parameters=PythonParameters(auth_token=auth_token),
        python_server_entry_point=entry_point
    )

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

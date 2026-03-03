import os
from typing import Union

from qcore import utils


def _update_params(d: dict, *u: dict) -> dict:
    """
    Update keys in a dictionary.

    Prevents removal of keys in a nested dict. Note that the same key will
    still be overwritten. The last dict in *u would preserve all its keys.

    Parameters
    ----------
    d : dict
        Original dictionary.
    *u : dict(s)
        Dictionary or dictionaries to update d with.

    Returns
    -------
    dict
        The dictionary d updated with the keys of *u.

    Examples
    --------
    >>> a = {'hf': {'hf_dt': 1, 'x': 2}}
    >>> b = {'hf': {'hf_dt': 3, 'y': 4}}
    >>> a.update(b)
    {'hf': {'hf_dt': 3, 'y': 4}}
    >>> _update_params(a, b)
    {'hf': {'hf_dt': 3, 'x': 2, 'y': 4}}
    """
    for uu in u:
        if uu:  # if uu is not empty
            for k, v in uu.items():
                if isinstance(v, dict):
                    d[k] = _update_params(d.get(k, {}), v)
                else:
                    d[k] = v
    return d


def load_sim_params(
    sim_yaml_path: Union[bool, str] = False,
    load_fault: Union[bool, str] = True,
    load_root: Union[bool, str] = True,
    load_vm: Union[bool, str] = True,
) -> dict:
    """Load all necessary parameters for a simulation.

    Parameters
    ----------
    sim_yaml_path : Union[bool, str]
        Path to sim_params.yaml or a falsy value to not load it.
    load_fault : Union[bool, str]
        Either True, the path to fault_params.yaml or a falsy value to not load it.
    load_root : Union[bool, str]
        Either True, the path to root_params.yaml or a false value to not load it.
    load_vm : Union[bool, str]
        Either True, the path to vm_params.yaml or a false value to not load it.

    Returns
    -------
    dict
        A dict object that contains all necessary params for a single simulation.
    """
    sim_params = {}
    fault_params = {}
    root_params = {}
    vm_params = {}

    if load_root is True or load_vm is True and not load_fault:
        load_fault = True  # root/vm_yamlpath in fault_yaml

    if sim_yaml_path:
        sim_params = utils.load_yaml(sim_yaml_path)
    elif load_fault is True:
        raise ValueError("For automated fault_params loading, sim_params must be set")

    if load_fault is True:
        fault_params = utils.load_yaml(sim_params["fault_yaml_path"])
    elif load_fault:
        fault_params = utils.load_yaml(load_fault)

    if load_root is True:
        root_params = utils.load_yaml(fault_params["root_yaml_path"])
    elif load_root:
        root_params = utils.load_yaml(load_root)

    if load_vm is True:
        vm_params = utils.load_yaml(
            os.path.join(fault_params["vel_mod_dir"], "vm_params.yaml")
        )
    elif load_vm:
        vm_params = utils.load_yaml(load_vm)

    return _update_params(vm_params, root_params, fault_params, sim_params)

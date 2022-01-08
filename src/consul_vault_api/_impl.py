import base64
import json
import os
from dataclasses import dataclass
from typing import Union, Dict, List

import dataclass_json
import requests

from dataclass_json import UNDEFINED

JsonableValue = Union[Dict[str, 'JsonableValue'], List['JsonableValue'], int, str, float, bool, None]


class ConsulClientV1:
    def __init__(self):
        self.__consul_http_addr = os.environ['CONSUL_HTTP_ADDR']

    def get(self, path, **kwargs) -> requests.Response:
        kwargs = {k: v for k, v in kwargs.items() if v is not UNDEFINED}
        resp = requests.get(self.__get_url(path), params=kwargs)
        resp.raise_for_status()
        return resp

    def get_str(self, path) -> str:
        resp = self.get(path)
        return resp.text

    def get_json(self, path) -> JsonableValue:
        return self.get(path).json()

    def put_str(self, path: str, value: str) -> requests.Response:
        resp = requests.put(self.__get_url(path), value)
        resp.raise_for_status()
        return resp

    def put_json(self, path: str, value: JsonableValue) -> requests.Response:
        return self.put_str(path, json.dumps(value))

    def kv_get(self, key: str,
               dc: str = UNDEFINED,
               recurse: bool = UNDEFINED,
               raw: bool = UNDEFINED,
               keys: bool = UNDEFINED,
               separator: str = UNDEFINED,
               ns: str = UNDEFINED) -> Union[List['ConsulKvEntry'], bytes, List[str]]:
        path = "/kv/" + key.lstrip('/')
        resp = self.get(path, dc=dc, recurse=recurse, raw=raw, keys=keys, separator=separator, ns=ns)
        if raw:
            return resp.content
        if keys:
            return resp.json()
        return dataclass_json.from_list(ConsulKvEntry, resp.json())

    def kv_set(self, key: str, value: str):
        path = "/kv/" + key.lstrip('/')
        response = self.put_str(path, value)
        return response.json()

    def __get_url(self, path):
        path = path.lstrip('/')
        return f"http://{self.__consul_http_addr}/v1/{path}"


@dataclass
class ConsulServiceDefinition:
    Name: str
    ID: str = UNDEFINED
    Tags: List[str] = UNDEFINED
    Address: str = UNDEFINED
    TaggedAddresses: Dict[str, str] = UNDEFINED
    Meta: Dict[str, str] = UNDEFINED
    Port: int = UNDEFINED
    Kind: str = UNDEFINED
    Proxy: 'Proxy' = UNDEFINED
    Connect: 'Connect' = UNDEFINED
    Check: 'ConsulCheck' = UNDEFINED
    Checks: List['ConsulCheck'] = UNDEFINED
    EnableTagOverride: bool = UNDEFINED
    Weights: bool = UNDEFINED


@dataclass
class Connect:
    Native: bool = UNDEFINED
    Proxy: 'Proxy' = UNDEFINED
    SidecarService: 'ServiceDefinition' = UNDEFINED


@dataclass
class ConsulCheck:
    """See https://www.consul.io/api/agent/check#parameters-1"""
    Name: str
    ID: str = UNDEFINED
    Namespace: str = UNDEFINED
    Interval: str = UNDEFINED
    Notes: str = UNDEFINED
    DeregisterCriticalServiceAfter: str = UNDEFINED
    Args: List[str] = UNDEFINED
    AliasNode: str = UNDEFINED
    AliasService: str = UNDEFINED
    DockerContainerID: str = UNDEFINED
    GRPC: str = UNDEFINED
    GRPCUseTLS: str = UNDEFINED
    H2PING: str = UNDEFINED
    H2PingUseTLS: bool = UNDEFINED
    HTTP: str = UNDEFINED
    Method: str = UNDEFINED
    Body: str = UNDEFINED
    Header: Dict[str, List[str]] = UNDEFINED
    Timeout: str = UNDEFINED
    OutputMaxSize: int = UNDEFINED
    TLSServerName: str = UNDEFINED
    TLSSkipVerify: bool = UNDEFINED
    TCP: str = UNDEFINED
    TTL: str = UNDEFINED
    ServiceID: str = UNDEFINED
    Status: str = UNDEFINED
    SuccessBeforePassing: int = UNDEFINED
    FailuresBeforeWarning: int = UNDEFINED
    FailuresBeforeCritical: int = UNDEFINED


@dataclass
class ConsulKvEntry:
    CreateIndex: int = UNDEFINED
    ModifyIndex: int = UNDEFINED
    LockIndex: int = UNDEFINED
    Key: str = UNDEFINED
    Flags: int = UNDEFINED
    Value: str = UNDEFINED
    Session: str = UNDEFINED

    def get_value(self) -> bytes:
        return base64.b64decode(self.Value)

    def get_value_str(self) -> str:
        return self.get_value().decode()

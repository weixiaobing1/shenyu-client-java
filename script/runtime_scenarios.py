# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#     http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

"""Protocol scenarios for the independent client source-runtime verifier."""

from contextlib import closing
import json
import os
import signal
import socket
import subprocess
from pathlib import Path

WEBSOCKET = "shenyu-examples/shenyu-examples-websocket/shenyu-example-spring-native-websocket"
WS_MAIN = "org.apache.shenyu.examples.websocket.TestNativeWebsocketApplication"
WS_PREFIX = "apache shenyu server send to Jack message : -> "
MVC_AUTOCONFIG = "org.apache.shenyu.springboot.starter.client.springmvc.ShenyuSpringMvc"
DUBBO_ROOT = "shenyu-examples/shenyu-examples-dubbo/shenyu-examples-apache-dubbo-service-"
DUBBO_SERVICE = "org.apache.shenyu.examples.dubbo.api.service.DubboTestService"


def modules(suite):
    if suite == "dubbo":
        return (DUBBO_ROOT + "annotation", DUBBO_ROOT + "xml")
    return (WEBSOCKET,) if suite == "websocket" else ()


def artifact(module, version):
    name = Path(module).name
    if module == DUBBO_ROOT + "annotation":
        name += "-" + version
    return name + ".jar"


def run(suite, scenario):
    if suite == "dubbo":
        plugin = next(row for row in scenario.page("/namespace-plugin", name="dubbo") if row["name"] == "dubbo")
        config = json.loads(plugin.get("config") or "{}")
        config["register"] = "zookeeper://" + scenario.zk_address
        scenario.admin_mutation("/namespace-plugin/" + plugin["id"], {
            "pluginId": plugin["pluginId"], "name": "dubbo", "namespaceId": plugin["namespaceId"],
            "sort": plugin["sort"], "enabled": True, "config": json.dumps(config),
        }, "PUT")
        for kind in ("annotation", "xml"):
            verify_dubbo(scenario, kind)
        return
    if suite != "websocket":
        raise ValueError(f"Unsupported runtime suite {suite}")
    scenario.enable_plugin("websocket")
    for mode, full, registry in (("annotation", False, False), ("isfull", True, False), ("isfull-zookeeper", True, True)):
        verify_websocket(scenario, mode, full, registry)


def reserve_port():
    bound = socket.socket()
    bound.bind(("127.0.0.1", 0))
    return bound


class Scenario:
    def __init__(self, runtime, report, classpaths, proxy, gateway_url, zk_port, page,
                 admin_data, enable_plugin, snapshot, request, wait_for, admin_mutation):
        self.runtime = runtime
        self.report = report
        self.classpaths = classpaths
        self.proxy = proxy
        self.gateway_url = gateway_url
        self.zk_address = f"127.0.0.1:{zk_port}"
        self.page = page
        self.admin_data = admin_data
        self.admin_mutation = admin_mutation
        self.enable_plugin = enable_plugin
        self.snapshot = snapshot
        self.request = request
        self.wait_for = wait_for

    def wait(self, label, condition, timeout=75):
        return self.wait_for(label, condition, self.runtime.processes, timeout)

    def remove(self, process, abrupt):
        if process.poll() is not None:
            raise AssertionError(f"Provider exited before lifecycle test: {process.returncode}")
        if abrupt:
            os.killpg(process.pid, signal.SIGKILL)
            process.wait(timeout=10)
        else:
            self.runtime.stop(process)
        self.runtime.processes[:] = [(label, item) for label, item in self.runtime.processes if item is not process]

    def selector(self, plugin, context):
        plugin_id = next(row["pluginId"] for row in self.page("/namespace-plugin", name=plugin) if row["name"] == plugin)
        return self.wait(f"{plugin} selector {context}", lambda: next(
            (row for row in self.page("/selector", pluginId=plugin_id, name=context) if row["name"] == context), None))

    def upstreams(self, selector_id):
        rows = self.admin_data("/selector/" + selector_id).get("discoveryUpstreams")
        if rows is None:
            return None
        return sorted(row["url"] for row in rows if row.get("status") == 0)

    def gateway_upstreams(self, context, plugin):
        rows = self.snapshot(self.runtime.output / "gateway.log", context, plugin)
        if rows is None:
            return None
        return sorted(row["url"] for row in rows if row.get("status") == 0)

    def probe(self, name, command):
        self.runtime.commands.append(command)
        result = subprocess.run(command, stdout=subprocess.PIPE, stderr=subprocess.STDOUT, text=True, timeout=30)
        log = self.runtime.output / (name + ".log")
        with log.open("a") as stream:
            stream.write(f"exit={result.returncode}\n{result.stdout}")
        return result


def verify_websocket(scenario, mode, full, registry):
    context = "/ws-" + mode
    uri = scenario.gateway_url.replace("http://", "ws://", 1) + context + "/myWebSocket?token=Jack"
    java = str(Path(os.environ["JAVA_HOME"]) / "bin" / "java") if "JAVA_HOME" in os.environ else "java"
    command = [java, str(scenario.runtime.root / "script/runtime/WebSocketProbe.java"), uri, WS_PREFIX]
    evidence = {"fixture": WEBSOCKET, "route": context + "/myWebSocket", "is_full": full, "discovery": "zookeeper" if registry else "local"}
    scenario.report["checks"]["websocket_" + mode] = evidence

    def start(name, bound):
        port = bound.getsockname()[1]
        args = [f"--server.port={port}", "--server.address=127.0.0.1",
                f"--shenyu.register.serverLists=http://127.0.0.1:{scenario.proxy.server.server_port}",
                f"--shenyu.common.contextPath={context}", f"--shenyu.common.appName={mode}", f"--shenyu.common.port={port}",
                "--shenyu.client.websocket.props.host=127.0.0.1",
                f"--shenyu.client.websocket.props.isFull={str(full).lower()}",
                "--spring.autoconfigure.exclude=" + ",".join(MVC_AUTOCONFIG + name for name in
                    ("ClientConfiguration", "ClientInfoRegisterConfiguration", "DiscoveryConfiguration")),
                f"--shenyu.discovery.type={'zookeeper' if registry else 'local'}"]
        if registry:
            args += ["--shenyu.discovery.enable=true", "--shenyu.discovery.register=true",
                     "--shenyu.discovery.protocol=ws://", f"--shenyu.discovery.serverList={scenario.zk_address}",
                     f"--shenyu.discovery.props.name={mode}"]
        bound.close()
        process = scenario.runtime.start(name, scenario.classpaths[WEBSOCKET], WS_MAIN, args)
        return process, f"127.0.0.1:{port}"

    def expect_upstreams(stage, expected):
        scenario.wait(f"{mode} {stage} gateway upstreams {expected}",
                      lambda: scenario.gateway_upstreams(context, "websocket") == sorted(expected))
        evidence[stage] = {"gateway_upstreams": expected}
        evidence[stage]["admin_upstreams"] = scenario.upstreams(selector_id)

    def expect_echo(stage, backend):
        log = scenario.runtime.output / f"{backend}.log"
        count = log.read_text().count("closed with status: CloseStatus[code=1000")
        scenario.wait(f"{mode} {stage} gateway echo and close",
                      lambda: scenario.probe(f"{mode}-{stage}", command).returncode == 0)
        scenario.wait(f"{mode} {stage} provider close",
                      lambda: log.read_text().count("closed with status: CloseStatus[code=1000") > count)
        evidence[stage]["echo_and_provider_close"] = True

    with closing(reserve_port()) as a, closing(reserve_port()) as b:
        label_a, label_b = f"{mode}-a", f"{mode}-b"
        first, url_a = start(label_a, a)
        selector = scenario.selector("websocket", context)
        selector_id = selector["id"]
        evidence["rules"] = scenario.wait(f"{mode} persisted WebSocket rules",
                                          lambda: scenario.page("/rule", selectorId=selector_id))
        expect_upstreams("registered", [url_a])
        expect_echo("registered", label_a)
        second, url_b = start(label_b, b)
        expect_upstreams("siblings", [url_a, url_b])
        scenario.remove(first, abrupt=False)
        expect_upstreams("graceful", [url_b])
        expect_echo("graceful", label_b)
        with closing(reserve_port()) as replacement:
            third, url_c = start(f"{mode}-replacement", replacement)
            expect_upstreams("replacement", [url_b, url_c])
        scenario.remove(second, abrupt=True)
        expect_upstreams("abrupt", [url_c])
        expect_echo("abrupt", f"{mode}-replacement")
        scenario.remove(third, abrupt=False)
        expect_upstreams("removed", [])
        failure = scenario.probe(f"{mode}-removed", command)
        if failure.returncode == 0 or "Failed WebSocket handshake" not in failure.stdout:
            raise AssertionError(f"{mode} gateway still upgraded the removed route: {failure.stdout}")
        uri_writes = scenario.proxy.matching("/shenyu-client/register-uri", context, "REGISTER")
        ws_writes = [entry for entry in uri_writes if str(entry["body"].get("rpcType")).lower() == "websocket"]
        if bool(ws_writes) == registry:
            raise AssertionError(f"{mode} unexpected local WebSocket URI registrations: {ws_writes}")
        evidence["local_uri_registrations"] = len(ws_writes)


def verify_dubbo(scenario, kind):
    context = "/dubbo-" + kind
    route = context + "/findById"
    module = DUBBO_ROOT + kind
    main_class = f"org.apache.shenyu.examples.apache.dubbo.service.{kind}.TestApacheDubbo{kind.title()}Application"
    evidence = {"fixture": module, "plugin": "apache-dubbo", "registry": "zookeeper", "route": route}
    scenario.report["checks"]["dubbo_" + kind] = evidence

    def start(suffix):
        with closing(reserve_port()) as http, closing(reserve_port()) as rpc:
            http_port = http.getsockname()[1]
            rpc_port = rpc.getsockname()[1]
            register_url = f"http://127.0.0.1:{scenario.proxy.server.server_port}"
            args = [f"--server.port={http_port}", "--server.address=127.0.0.1",
                    f"--shenyu.register.serverLists={register_url}",
                    f"--shenyu.client.dubbo.props.contextPath={context}", f"--shenyu.client.dubbo.props.appName={kind}",
                    f"--dubbo.registry.address=zookeeper://{scenario.zk_address}",
                    f"--dubbo.registry.port={rpc_port}", f"--dubbo.protocol.port={rpc_port}"]
            http.close()
            rpc.close()
        process = scenario.runtime.start(f"dubbo-{kind}-{suffix}", scenario.classpaths[module], main_class, args)
        return process, rpc_port

    def echo():
        status, text, _ = scenario.request(scenario.gateway_url + route + "?id=8422")
        with (scenario.runtime.output / f"dubbo-{kind}-calls.log").open("a") as stream:
            stream.write(f"HTTP {status} {text[:2000]}\n")
        if status != 200:
            return None
        data = json.loads(text)
        expected = {"id": "8422", "name": "hello world shenyu Apache, findById"}
        return data if isinstance(data, dict) and all(data.get(key) == value for key, value in expected.items()) else None

    def upstreams(stage, ports):
        def active():
            snapshot = scenario.gateway_upstreams(context, "dubbo")
            if snapshot is None:
                return None
            return sorted(int(url.rsplit(":", 1)[1]) for url in snapshot)
        scenario.wait(f"Dubbo {kind} {stage} gateway upstreams {ports}", lambda: active() == sorted(ports))
        evidence[stage] = {"gateway_ports": ports, "admin_upstreams": scenario.upstreams(selector["id"])}

    a, port_a = start("a")
    selector = scenario.selector("dubbo", context)
    evidence["metadata"] = scenario.wait(f"Dubbo {kind} persisted method metadata", lambda: next(
        (row for row in scenario.page("/meta-data/queryList") if row["path"] == route
         and row["methodName"] == "findById" and row["serviceName"] == DUBBO_SERVICE), None))
    upstreams("registered", [port_a])
    evidence["registered"]["response"] = scenario.wait(f"Dubbo {kind} registered invocation", echo)
    b, port_b = start("b")
    upstreams("siblings", [port_a, port_b])
    scenario.remove(a, abrupt=False)
    upstreams("graceful", [port_b])
    evidence["graceful"]["response"] = scenario.wait(f"Dubbo {kind} surviving sibling after graceful stop", echo)
    c, port_c = start("replacement")
    upstreams("replacement", [port_b, port_c])
    scenario.remove(b, abrupt=True)
    upstreams("abrupt", [port_c])
    evidence["abrupt"]["response"] = scenario.wait(f"Dubbo {kind} surviving sibling after kill", echo)
    scenario.remove(c, abrupt=False)
    upstreams("removed", [])
    scenario.wait(f"Dubbo {kind} gateway stopped routing", lambda: echo() is None)

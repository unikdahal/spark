#!/usr/bin/env python3
#
# Licensed to the Apache Software Foundation (ASF) under one or more
# contributor license agreements.  See the NOTICE file distributed with
# this work for additional information regarding copyright ownership.
# The ASF licenses this file to You under the Apache License, Version 2.0
# (the "License"); you may not use this file except in compliance with
# the License.  You may obtain a copy of the License at
#
#    http://www.apache.org/licenses/LICENSE-2.0
#
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.
#

"""Own isolated Celeborn services for the native proof; never attach to existing services."""

import os
from pathlib import Path
import socket
import signal
import subprocess
import sys
import time
import uuid


def free_port():
    with socket.socket() as sock:
        sock.bind(("127.0.0.1", 0))
        return sock.getsockname()[1]


def main():
    root = Path(sys.argv[1]).resolve()
    root.mkdir(parents=True, exist_ok=False)
    home = Path(os.environ["CELEBORN_HOME"]).resolve()
    endpoint = root / "owner.properties"
    worker_data = root / "worker-data"
    worker_data.mkdir()
    master_port = free_port()
    owner_port = free_port()
    conf = root / "celeborn.conf"
    conf.write_text("\n".join([
        f"celeborn.master.endpoints 127.0.0.1:{master_port}",
        "celeborn.master.ha.enabled false",
        f"celeborn.master.http.port {free_port()}",
        f"celeborn.worker.http.port {free_port()}",
        f"celeborn.worker.storage.dirs {worker_data}:capacity=1G:disktype=HDD",
        "celeborn.worker.storage.disk.reserve.size 1m",
        "celeborn.worker.storage.workingDir retained-proof",
        "celeborn.worker.directMemoryRatioForMemoryFileStorage 0",
        "celeborn.client.push.replicate.enabled false",
        "celeborn.metrics.enabled false",
        f"celeborn.retainedShuffle.endpointFile {endpoint}",
        "",
    ]))
    log_config = root / "log4j2.xml"
    log_config.write_text(
        '<Configuration><Appenders><Console name="console" target="SYSTEM_OUT">'
        '<PatternLayout pattern="%d %p %c: %m%n%ex"/></Console></Appenders>'
        '<Loggers><Root level="info"><AppenderRef ref="console"/></Root></Loggers>'
        '</Configuration>')
    # Match the Java module access configured by Celeborn's normal service launchers.
    opens = ["java.lang", "java.lang.invoke", "java.lang.reflect", "java.io", "java.net",
             "java.nio", "java.util", "java.util.concurrent", "java.util.concurrent.atomic",
             "jdk.internal.misc", "sun.nio.ch", "sun.nio.cs", "sun.security.action",
             "sun.util.calendar"]
    java_options = [f"--add-opens=java.base/{name}=ALL-UNNAMED" for name in opens]
    java_options += ["--add-opens=java.security.jgss/sun.security.krb5=ALL-UNNAMED",
                     f"-Dlog4j2.configurationFile={log_config}"]
    # RPC bind addresses and advertised worker locations must name the same interface.
    service_env = dict(os.environ, CELEBORN_LOCAL_HOSTNAME="127.0.0.1",
                       SPARK_LOCAL_IP="127.0.0.1")
    processes = []
    streams = []
    java = str(Path(os.environ["JAVA_HOME"]) / "bin/java")

    def start(name, main_class, extra, suffix=""):
        log = (root / f"{name}{suffix}.log").open("w")
        streams.append(log)
        command = [java, "-Xmx768m", "-XX:MaxDirectMemorySize=768m"] + java_options + [
                   "-cp", f"{home}/conf:{home}/{name}-jars/*:{home}/jars/*",
                   main_class, "--host", "127.0.0.1", "--properties-file", str(conf)] + extra
        process = subprocess.Popen(command, stdout=log, stderr=subprocess.STDOUT,
                                   env=service_env)
        processes.append(process)
        return process

    def wait_for(predicate, description, timeout=90):
        deadline = time.monotonic() + timeout
        while time.monotonic() < deadline:
            for process in processes:
                if process.poll() is not None:
                    raise RuntimeError(f"service {process.pid} exited: {process.returncode}")
            if predicate():
                return
            time.sleep(0.5)
        raise TimeoutError(description)

    def listening(port):
        try:
            with socket.create_connection(("127.0.0.1", port), timeout=0.5):
                return True
        except OSError:
            return False

    try:
        start("master", "org.apache.celeborn.service.deploy.master.Master",
              ["--port", str(master_port)])
        wait_for(lambda: listening(master_port), "master startup")
        start("worker", "org.apache.celeborn.service.deploy.worker.Worker", [])
        wait_for(lambda: "Register worker successfully." in
                 (root / "worker.log").read_text(errors="replace"), "worker registration")
        owner = start("lifecycle-manager",
              "org.apache.celeborn.server.lifecyclemanager.LifecycleManagerDaemon",
              ["--app-id", "cold-" + uuid.uuid4().hex,
               "--master-endpoints", f"127.0.0.1:{master_port}", "--port", str(owner_port)])
        wait_for(lambda: endpoint.is_file() and endpoint.stat().st_size > 0,
                 "retention control endpoint publication")
        env = dict(service_env, CELEBORN_RETAINED_ENDPOINT_FILE=str(endpoint),
                   CELEBORN_PROOF_WORKER_ROOT=str(worker_data / "retained-proof"),
                   CELEBORN_PROOF_CONTROL_ROOT=str(root))
        child = subprocess.Popen(
            ["bash", "dev/shuffle-recovery/celeborn-native-spike/cold-process.sh",
             str(root / "drivers")], env=env)
        paused = False
        try:
            while child.poll() is None:
                if (root / "restart-owner").exists() and not (root / "owner-restarted").exists():
                    assert not paused, "owner cannot be paused and restarted simultaneously"
                    old_endpoint = endpoint.read_text()
                    (root / "owner-before-restart.properties").write_text(old_endpoint)
                    owner.terminate()
                    try:
                        owner.wait(timeout=20)
                    except subprocess.TimeoutExpired:
                        owner.kill()
                        owner.wait()
                    processes.remove(owner)
                    endpoint.unlink()
                    owner = start("lifecycle-manager",
                        "org.apache.celeborn.server.lifecyclemanager.LifecycleManagerDaemon",
                        ["--app-id", "cold-" + uuid.uuid4().hex,
                         "--master-endpoints", f"127.0.0.1:{master_port}",
                         "--port", str(owner_port)], suffix="-restarted")
                    wait_for(lambda: endpoint.is_file() and endpoint.stat().st_size > 0,
                             "restarted owner endpoint publication")
                    assert endpoint.read_text() != old_endpoint
                    (root / "owner-restarted").write_text("new incarnation on the same port\n")
                if (root / "pause-owner").exists() and not (root / "owner-paused").exists():
                    owner.send_signal(signal.SIGSTOP)
                    paused = True
                    (root / "owner-paused").write_text("paused the harness-owned lifecycle JVM\n")
                if paused and (root / "resume-owner").exists():
                    owner.send_signal(signal.SIGCONT)
                    paused = False
                    (root / "owner-resumed").write_text("resumed the same owner incarnation\n")
                time.sleep(0.2)
            if child.returncode != 0:
                raise subprocess.CalledProcessError(child.returncode, child.args)
        finally:
            if paused and owner.poll() is None:
                owner.send_signal(signal.SIGCONT)
            if child.poll() is None:
                child.terminate()
                try:
                    child.wait(timeout=10)
                except subprocess.TimeoutExpired:
                    child.kill()
                    child.wait()
    finally:
        for process in reversed(processes):
            if process.poll() is None:
                process.terminate()
        deadline = time.monotonic() + 20
        for process in reversed(processes):
            try:
                process.wait(timeout=max(0.1, deadline - time.monotonic()))
            except subprocess.TimeoutExpired:
                process.kill()
                process.wait()
        for stream in streams:
            stream.close()


if __name__ == "__main__":
    main()

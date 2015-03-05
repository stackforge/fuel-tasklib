# -*- coding: utf-8 -*-

#    Copyright 2015 Mirantis, Inc.
#
#    Licensed under the Apache License, Version 2.0 (the "License"); you may
#    not use this file except in compliance with the License. You may obtain
#    a copy of the License at
#
#         http://www.apache.org/licenses/LICENSE-2.0
#
#    Unless required by applicable law or agreed to in writing, software
#    distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
#    WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
#    License for the specific language governing permissions and limitations
#    under the License.

import jsonschema

from tasks_validator import  graph
from tasks_validator.schemas import VERSIONS_SCHEMAS_MAP


class TasksValidator(object):

    def __init__(self, tasks, version):
        self.version = version
        self.tasks = tasks
        self.graph = graph.DeploymentGraph(tasks)

    def validate_schema(self):
        """Validate tasks schema

        :raise: jsonschema.ValidationError
        """
        checker = jsonschema.FormatChecker()
        schema = VERSIONS_SCHEMAS_MAP.get(self.version)().tasks_schema
        jsonschema.validate(self.tasks, schema, format_checker=checker)

    def validate_graph(self):
        """Validate graph if is executable completely by fuel nailgun

        :raise: ValueEror when one of requirements is not satisfied
        """
        msgs = []

        # deployment graph should be without cycles
        cycles = self.graph.find_cycles()
        if len(cycles):
            msgs.append('Graph is not acyclic. Cycles: {0}'.format(cycles))

        # graph should be connected to execute all tasks
        if not self.graph.is_connected():
            msgs.append('Graph is not connected.')

        if msgs:
            raise ValueError("Graph validation fail: {0}".format(msgs))

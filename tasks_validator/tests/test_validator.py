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

from unittest2.case import TestCase

from tasks_validator import validator


class TestValidator61(TestCase):

    def setUp(self):
        self.tasks = [
            {'id': 'pre_deployment_start',
             'type': 'stage'},
            {'id': 'pre_deployment_end',
             'requires': ['pre_deployment_start'],
             'type': 'stage'},
            {'id': 'deploy_start',
             'requires': ['pre_deployment_end'],
             'type': 'stage'},
            {'id': 'deploy_end',
             'requires': ['deploy_start'],
             'type': 'stage'},
            {'id': 'post_deployment_start',
             'requires': ['deploy_end'],
             'type': 'stage'},
            {'id': 'post_deployment_end',
             'requires': ['post_deployment_start'],
             'type': 'stage'}]

    def test_validate_schema(self):
        valid_tasks = validator.TasksValidator(self.tasks, "6.1")
        valid_tasks.validate_schema()

    def test_wrong_schema(self):
        self.tasks.append({'id': 'wrong',
                           'type': 'non existing'})
        valid_tasks = validator.TasksValidator(self.tasks, "6.1")
        self.assertRaises(jsonschema.ValidationError,
                          valid_tasks.validate_schema)

    def test_validate_graph(self):
        valid_tasks = validator.TasksValidator(self.tasks, "6.1")
        valid_tasks.validate_graph()

    def test_validate_cyclic_graph(self):
        self.tasks.append({'id': 'post_deployment_part',
                           'type': 'stage',
                           'requires': ['post_deployment_start'],
                           'required_for': ['pre_deployment_start']})
        valid_tasks = validator.TasksValidator(self.tasks, "6.1")
        self.assertRaises(ValueError,
                          valid_tasks.validate_graph)

    def test_validate_not_connected_graph(self):
        self.tasks.append({'id': 'post_deployment_part',
                           'type': 'stage'})
        valid_tasks = validator.TasksValidator(self.tasks, "6.1")
        self.assertRaises(ValueError,
                          valid_tasks.validate_graph)

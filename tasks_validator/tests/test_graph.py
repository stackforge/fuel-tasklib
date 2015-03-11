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

try:
    from collections import OrderedDict
except ImportError:
    from ordereddict import OrderedDict
from unittest2.case import TestCase

from tasks_validator import graph


TASKS = [
    {
        'id': 'pre_deployment_start',
        'type': 'stage'
    },
    {
        'id': 'pre_deployment',
        'type': 'stage',
        'requires': ['pre_deployment_start']
    },
    {
        'id': 'deploy_start',
        'type': 'stage',
        'requires': ['pre_deployment']
    },
    {
        'id': 'deploy_end',
        'type': 'stage',
        'requires': ['deploy_start']
    },
    {
        'id': 'primary-controller',
        'type': 'group',
        'role': ['primary-controller'],
        'required_for': ['deploy_end'],
        'requires': ['deploy_start'],
        'parameters': {
            'strategy':
                {'type': 'one_by_one'}
        }
    },
    {
        'id': 'controller',
        'type': 'group',
        'role': ['controller'],
        'requires': ['primary-controller'],
        'required_for': ['deploy_end'],
        'parameters': {
            'strategy': {
                'type': 'parallel',
                'amount': 2
            }
        }
    },
    {
        'id': 'cinder',
        'type': 'group',
        'role': ['cinder'],
        'requires': ['controller'],
        'required_for': ['deploy_end'],
        'parameters': {
            'strategy': {
                'type': 'parallel'}
            }
    },
    {
        'id': 'compute',
        'type': 'group',
        'role': ['compute'],
        'requires': ['controller'],
        'required_for': ['deploy_end'],
        'parameters': {
            'strategy': {
                'type': 'parallel'
            },
        }
    },
    {
        'id': 'network',
        'type': 'group',
        'role': ['network'],
        'requires': ['controller'],
        'required_for': ['compute', 'deploy_end'],
        'parameters': {
            'strategy': {
                'type': 'parallel'}
        }
    }]

SUBTASKS = [
    {
        'id': 'install_controller',
        'type': 'puppet',
        'requires': ['setup_network'],
        'groups': ['controller', 'primary-controller'],
        'required_for': ['deploy_end'],
        'parameters':{
            'puppet_manifests': '/etc/puppet/manifests/controller.pp',
            'puppet_modules': '/etc/puppet/modules',
            'timeout': 360
        }
    },
    {
        'id': 'setup_network',
        'type': 'puppet',
        'groups': ['controller', 'primary-controller'],
        'required_for': ['deploy_end'],
        'requires': ['deploy_start'],
        'parameters':{
            'puppet_manifest': 'run_setup_network.pp',
            'puppet_modules': '/etc/puppet',
            'timeout': 120
        }
    },
    {
        'id': 'setup_anything',
        'requires': ['pre_deployment_start'],
        'required_for': ['pre_deployment'],
        'type': 'shell'
    },
    {
        'id': 'setup_more_stuff',
        'type': 'shell',
        'requires_for': ['pre_deployment'],
        'requires': ['setup_anything']
    }]


class TestGraphDependencies(TestCase):

    def setUp(self):
        super(TestGraphDependencies, self).setUp()
        self.tasks = TASKS
        self.subtasks = SUBTASKS
        self.graph = graph.DeploymentGraph()

    def test_build_deployment_graph(self):
        self.graph.add_tasks(self.tasks)
        roles = self.graph.get_groups_subgraph()
        topology_by_id = [item['id'] for item in roles.topology]
        self.assertEqual(
            topology_by_id[:2], ['primary-controller', 'controller'])
        network_pos = topology_by_id.index('network')
        compute_pos = topology_by_id.index('compute')
        cinder_pos = topology_by_id.index('cinder')
        controller_pos = topology_by_id.index('controller')
        # we don't have constraint on certain order between cinder and network
        # therefore there should not be one
        self.assertGreater(compute_pos, network_pos)
        self.assertGreater(cinder_pos, controller_pos)

    def test_subtasks_in_correct_order(self):
        self.graph.add_tasks(self.tasks + self.subtasks)
        subtask_graph = self.graph.get_tasks('controller')
        topology_by_id = [item['id'] for item in subtask_graph.topology]
        self.assertItemsEqual(
            topology_by_id,
            ['setup_network', 'install_controller'])


class TestGraphs(TestCase):

    def test_connectability(self):
        tasks = [
            {'id': 'pre_deployment_start',
             'type': 'stage'},
            {'id': 'pre_deployment_end',
             'type': 'stage',
             'requires': ['pre_deployment_start']},
            {'id': 'deploy_start',
             'type': 'stage'}]
        tasks_graph = graph.DeploymentGraph()
        tasks_graph.add_tasks(tasks)
        self.assertFalse(tasks_graph.is_connected())

    def test_cyclic(self):
        tasks = [
            {'id': 'pre_deployment_start',
             'type': 'stage'},
            {'id': 'pre_deployment_end',
             'type': 'stage',
             'requires': ['pre_deployment_start']},
            {'id': 'deploy_start',
             'type': 'stage',
             'requires': ['pre_deployment_end'],
             'required_for': ['pre_deployment_start']}]
        tasks_graph = graph.DeploymentGraph()
        tasks_graph.add_tasks(tasks)
        cycles = tasks_graph.find_cycles()
        self.assertEqual(len(cycles), 1)
        self.assertItemsEqual(cycles[0], ['deploy_start',
                                          'pre_deployment_start',
                                          'pre_deployment_end'])
        self.assertIsInstance(tasks_graph.node, OrderedDict)

    def test_empty_nodes(self):
        tasks = [
            {'id': 'pre_deployment_start',
             'type': 'stage',
             'requires': ['empty_node']},
            {'id': 'pre_deployment_end',
             'type': 'stage',
             'requires': ['pre_deployment_start']},
            {'id': 'deploy_start',
             'type': 'stage',
             'requires': ['empty_node_2']}]
        tasks_graph = graph.DeploymentGraph()
        tasks_graph.add_tasks(tasks)
        self.assertItemsEqual(tasks_graph.find_empty_nodes(), ['empty_node',
                                                               'empty_node_2'])

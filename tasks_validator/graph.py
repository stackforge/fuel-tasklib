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

import networkx as nx

from tasks_validator import consts


class DeploymentGraph(nx.DiGraph):
    """DirectedGraph that is used to generate configuration for orchestrator.

    General task format

    id: string
    type: string - one of - role, stage, puppet, shell, upload_file, sync
    required_for: direct dependencies
    requires: reverse dependencies

    groups: direct dependencies for different levels
    tasks: reverse dependencies for different levels

    stage: direct dependency
    parameters: specific for each task type parameters

    """

    def __init__(self, *args, **kwargs):
        super(DeploymentGraph, self).__init__(*args, **kwargs)

    def add_tasks(self, tasks):
        for task in tasks:
            self.add_task(task)

    def add_task(self, task):
        task_id = task.get('id')
        if task_id:
            self.add_node(task_id, **task)
            if 'required_for' in task:
                for req in task['required_for']:
                    self.add_edge(task_id, req)
            if 'requires' in task:
                for req in task['requires']:
                    self.add_edge(req, task_id)

            if 'groups' in task:
                for req in task['groups']:
                    self.add_edge(task_id, req)
            if 'tasks' in task:
                for req in task['tasks']:
                    self.add_edge(req, task_id)

    def find_cycles(self):
        """Find cycles in graph.

        :return: list of cycles in graph
        """
        cycles = []
        for cycle in nx.simple_cycles(self):
            cycles.append(cycle)
        return cycles

    def is_connected(self):
        """Check if graph is connected.

        :return: bool
        """
        return nx.is_weakly_connected(self)

    def get_next_groups(self, processed_nodes):
        """Get nodes that have predecessors in processed_nodes list.

        All predecessors should be taken into account, not only direct
        parents

        :param processed_nodes: set of nodes names
        :returns: list of nodes names
        """
        result = []
        for node in self.nodes():
            if node in processed_nodes:
                continue

            predecessors = nx.dfs_predecessors(self.reverse(), node)
            if (set(predecessors.keys()) <= processed_nodes):
                result.append(node)

        return result

    def get_groups_subgraph(self):
        roles = [t['id'] for t in self.node.values()
                 if t['type'] == consts.ORCHESTRATOR_TASK_TYPES.group]
        return self.subgraph(roles)

    def get_tasks(self, group_name):
        tasks = []
        for task in self.predecessors(group_name):
            if self.node[task]['type'] not in consts.INTERNAL_TASKS:
                tasks.append(task)
        return self.subgraph(tasks)

    @property
    def topology(self):
        return map(lambda t: self.node[t], nx.topological_sort(self))

    def make_void_task(self, task):
        """Make some task in graph simple void

        We can not just remove node because it also stores edges, that connects
        graph in correct order

        :param task_id: id of the node in graph
        """
        if task['type'] in consts.INTERNAL_TASKS:
            return

        task['type'] = consts.ORCHESTRATOR_TASK_TYPES.void

    def only_tasks(self, task_ids):
        """Leave only tasks that are specified in request.

        :param task_ids: list of task ids
        """
        if not task_ids:
            return

        for task in self.node.values():
            if task.get('id') not in task_ids:
                self.make_void_task(task)

    def find_subgraph(self, start=None, end=None):
        """Find subgraph by provided start and end endpoints

        :param end: string
        :param start: string
        :returns: DeploymentGraph instance (subgraph from original)
        """
        working_graph = self

        if start:
            # simply traverse starting from root,
            # A->B, B->C, B->D, C->E
            working_graph = self.subgraph(
                nx.dfs_postorder_nodes(working_graph, start))

        if end:
            # nx.dfs_postorder_nodes traverses graph from specified point
            # to the end by following successors, here is example:
            # A->B, C->D, B->D , and we want to traverse up to the D
            # for this we need to reverse graph and make it
            # B->A, D->C, D->B and use dfs_postorder
            working_graph = self.subgraph(nx.dfs_postorder_nodes(
                working_graph.reverse(), end))

        return working_graph

    def find_empty_nodes(self):
        """Find empty nodes in graph.

        :return: list of empty nodes in graph
        """
        empty_nodes = []
        for node_name, node in self.node.items():
            if node == {}:
                empty_nodes.append(node_name)
        return empty_nodes

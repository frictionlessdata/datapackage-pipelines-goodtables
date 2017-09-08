import os
import unittest

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_goodtables.processors

import logging
log = logging.getLogger(__name__)

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')


class TestValidateProcessor(unittest.TestCase):

    def test_validate_processor_no_resources_no_params(self):

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {}

        # Path to the processor we want to test
        processor_dir = os.path.dirname(
            datapackage_pipelines_goodtables.processors.__file__)
        processor_path = os.path.join(processor_dir, 'validate.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, []))

        spew_dp = spew_args[0]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        # No resources
        assert len(dp_resources) == 0

    def test_validate_processor_no_resources_with_params(self):

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'fail_on_error': True,
            'fail_on_warn': True
        }

        # Path to the processor we want to test
        processor_dir = os.path.dirname(
            datapackage_pipelines_goodtables.processors.__file__)
        processor_path = os.path.join(processor_dir, 'validate.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage, []))

        spew_dp = spew_args[0]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        # No resources
        assert len(dp_resources) == 0

    def test_validate_processor_one_resource(self):

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'my-resource',
                'schema': {
                    'fields': []
                }
            }]
        }
        params = {}

        def row_yielder():
            yield {'id': 1, 'name': 'english'}
            yield {'name': 'english', 'thing': 'what'}

        # Path to the processor we want to test
        processor_dir = os.path.dirname(
            datapackage_pipelines_goodtables.processors.__file__)
        processor_path = os.path.join(processor_dir, 'validate.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage,
                                            iter([row_yielder()])))

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        # Asserts for the datapackage
        dp_resources = spew_dp['resources']
        # No resources
        assert len(dp_resources) == 1
        assert dp_resources[0]['name'] == 'my-resource'

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        assert len(spew_res_iter_contents) == 1
        assert list(spew_res_iter_contents[0]) == \
            [
                {'id': 1, 'name': 'english'},
                {'name': 'english', 'thing': 'what'}
            ]

    def test_validate_processor_one_resource_fail_on_warn(self):

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'my-resource',
                'schema': {
                    'fields': []  # no schema will cause goodtables warning
                }
            }]
        }
        params = {'fail_on_warn': True}

        def row_yielder():
            yield {'id': 1, 'name': 'english'}

        # Path to the processor we want to test
        processor_dir = os.path.dirname(
            datapackage_pipelines_goodtables.processors.__file__)
        processor_path = os.path.join(processor_dir, 'validate.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage,
                                            iter([row_yielder()])))

        spew_res_iter = spew_args[1]

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        error_msg = 'Datapackage failed Goodtables validation. '\
                    'See log for details.'
        # listing rows in resource will trigger validation
        with self.assertRaises(RuntimeError) as cm:
            list(spew_res_iter_contents[0])
        self.assertEqual(str(cm.exception), error_msg)

    def test_validate_processor_one_resource_fail_on_error(self):

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'my-resource',
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'string'},
                        {'name': 'name', 'type': 'string'}
                    ]
                }
            }]
        }
        params = {'fail_on_warn': True}

        def row_yielder():
            # id not a string causes goodtables error
            yield {'id': 1, 'name': 'english'}

        # Path to the processor we want to test
        processor_dir = os.path.dirname(
            datapackage_pipelines_goodtables.processors.__file__)
        processor_path = os.path.join(processor_dir, 'validate.py')

        # Trigger the processor with our mock `ingest` and capture what it will
        # returned to `spew`.
        spew_args, _ = mock_processor_test(processor_path,
                                           (params, datapackage,
                                            iter([row_yielder()])))

        spew_res_iter = spew_args[1]

        # Asserts for the res_iter
        spew_res_iter_contents = list(spew_res_iter)
        error_msg = 'Datapackage failed Goodtables validation. '\
                    'See log for details.'
        # listing rows in resource will trigger validation
        with self.assertRaises(RuntimeError) as cm:
            list(spew_res_iter_contents[0])
        self.assertEqual(str(cm.exception), error_msg)

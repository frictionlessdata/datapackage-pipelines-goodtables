import io
import os
import json
import unittest

from datapackage_pipelines.utilities.lib_test_helpers import (
    mock_processor_test
)

import datapackage_pipelines_goodtables.processors

import logging
log = logging.getLogger(__name__)

ROOT_PATH = os.path.join(os.path.dirname(__file__), '..')


class TestValidateProcessor(unittest.TestCase):

    @classmethod
    def get_base_path(cls):
        '''Making this a readonly property prevents it being overridden, and
        subsequently deleted!'''
        return 'tests/delete_tests'

    def tearDown(self):
        # Remove all files under test path
        files = os.listdir(self.get_base_path())
        for f in files:
            os.remove(os.path.join(self.get_base_path(), f))

    @classmethod
    def setUpClass(cls):
        # Create directory for writing test files
        if not os.path.exists(cls.get_base_path()):
            os.mkdir(cls.get_base_path())

    @classmethod
    def tearDownClass(cls):
        # Remove test directory
        if os.path.exists(cls.get_base_path()):
            os.rmdir(cls.get_base_path())

    def test_validate_processor_no_resources_no_params(self):
        '''No resources, so no reports added to datapackage.'''

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'datapackage_reports_path': 'reports'
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
        assert len(spew_dp['reports']) == 0

    def test_validate_processor_no_resources_with_params(self):

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': []
        }
        params = {
            'fail_on_error': True,
            'datapackage_reports_path': 'reports'
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
        assert len(spew_dp['reports']) == 0

    def test_validate_processor_valid_resource(self):

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'my-resource',
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'integer'},
                        {'name': 'name', 'type': 'string'}
                    ]
                }
            }]
        }

        report_dir = '{}'.format(self.get_base_path())

        params = {
            'reports_path': report_dir,
            'datapackage_reports_path': 'reports',
        }

        def row_yielder():
            yield {'id': 1, 'name': 'english'}
            yield {'id': 2, 'name': 'german'}

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

        spew_res_iter_contents = list(spew_res_iter)
        # listing rows in resource will trigger validation
        list(spew_res_iter_contents[0])

        # Asserts for the datapackage
        reports = spew_dp['reports']
        assert len(reports) == 1
        assert reports[0]['resource'] == 'my-resource'
        assert reports[0]['reportType'] == 'goodtables'
        assert reports[0]['path'] == 'reports/my-resource.json'

        with io.open('{}/my-resource.json'.format(report_dir), 'r') as f:
            report_json = json.loads(f.read())
            assert report_json['valid'] is True

    def test_validate_processor_valid_resource_no_report(self):
        '''Don't write report if `write_report` is False.'''

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'valid-no-report',
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'integer'},
                        {'name': 'name', 'type': 'string'}
                    ]
                }
            }]
        }

        report_dir = '{}'.format(self.get_base_path())

        params = {
            'reports_path': report_dir,
            'write_report': False
        }

        def row_yielder():
            yield {'id': 1, 'name': 'english'}
            yield {'id': 2, 'name': 'german'}

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

        spew_res_iter_contents = list(spew_res_iter)
        # listing rows in resource will trigger validation
        list(spew_res_iter_contents[0])

        # No `reports` property in datapackage
        assert 'reports' not in spew_dp

        assert not os.path.isfile('{}/valid-no-report.json'.format(report_dir))

    def test_validate_processor_invalid_resource_fail_on_error(self):
        '''Fail on error and write report.'''
        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'resource-fail-write',
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'string'},
                        {'name': 'name', 'type': 'string'}
                    ]
                }
            }]
        }
        report_dir = '{}'.format(self.get_base_path())

        params = {
            'reports_path': report_dir,
            'fail_on_error': True
        }

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
        error_msg = 'Datapackage resource \'resource-fail-write\' ' \
                    'failed Goodtables validation. See report for details: ' \
                    'tests/delete_tests/resource-fail-write.json'
        # listing rows in resource will trigger validation
        with self.assertRaises(RuntimeError) as cm:
            list(spew_res_iter_contents[0])
        self.assertEqual(str(cm.exception), error_msg)

        with io.open('{}/resource-fail-write.json'.format(report_dir), 'r') as f:
            report_json = json.loads(f.read())
            assert report_json['valid'] is False

    def test_validate_processor_invalid_resource_fail_on_error_no_report(self):
        '''Fail on error but don't write report.'''
        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'resource-fail-no-write',
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'string'},
                        {'name': 'name', 'type': 'string'}
                    ]
                }
            }]
        }
        report_dir = '{}'.format(self.get_base_path())

        params = {
            'reports_path': report_dir,
            'fail_on_error': True,
            'write_report': False
        }

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
        error_msg = 'Datapackage resource \'resource-fail-no-write\' ' \
                    'failed Goodtables validation.'
        # listing rows in resource will trigger validation
        with self.assertRaises(RuntimeError) as cm:
            list(spew_res_iter_contents[0])
        self.assertEqual(str(cm.exception), error_msg)

        assert not os.path.isfile('{}/resource-fail-no-write.json'.format(report_dir))

    def test_validate_processor_invalid_resource_dont_fail(self):
        '''Don't fail on error. Report is written.'''

        # input arguments used by our mock `ingest`
        datapackage = {
            'name': 'my-datapackage',
            'project': 'my-project',
            'resources': [{
                'name': 'resource-dont-fail-write',
                'schema': {
                    'fields': [
                        {'name': 'id', 'type': 'string'},
                        {'name': 'name', 'type': 'string'}
                    ]
                }
            }]
        }

        report_dir = '{}'.format(self.get_base_path())

        params = {
            'reports_path': report_dir,
            'datapackage_reports_path': 'reports',
            'fail_on_error': False
        }

        def row_yielder():
            # row will fail schema validation
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

        spew_dp = spew_args[0]
        spew_res_iter = spew_args[1]

        spew_res_iter_contents = list(spew_res_iter)
        # listing rows in resource will trigger validation
        list(spew_res_iter_contents[0])

        # Asserts for the datapackage
        reports = spew_dp['reports']
        assert len(reports) == 1
        assert reports[0]['resource'] == 'resource-dont-fail-write'
        assert reports[0]['reportType'] == 'goodtables'
        assert reports[0]['path'] == 'reports/resource-dont-fail-write.json'

        with io.open('{}/resource-dont-fail-write.json'.format(report_dir),
                     'r') as f:
            report_json = json.loads(f.read())
            assert report_json['valid'] is False

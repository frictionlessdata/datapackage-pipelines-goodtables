import os
import datetime
import unittest
from decimal import Decimal

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

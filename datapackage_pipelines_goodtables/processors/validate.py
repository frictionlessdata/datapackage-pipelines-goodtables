import os
import io
import json
import itertools

from datapackage_pipelines.wrapper import ingest, spew
import goodtables

import logging
log = logging.getLogger(__name__)


parameters, datapackage, res_iter = ingest()

fail_on_error = parameters.get('fail_on_error', True)
write_report = parameters.get('write_report', True)
goodtables_options = parameters.get('goodtables', {})
reports_path = parameters.get('reports_path', 'reports')


def process_resources(res_iter_, datapackage, goodtables_options):

    def _get_row_value(r):
        if hasattr(r, '_evaluate'):
            return r._evaluate()
        else:
            return r

    def _validate_resource(res, dp_res):
        evaluated_rows, rows = itertools.tee(res)
        evaluated_rows = list(_get_row_value(r) for r in evaluated_rows)
        validate_options = {
            'schema': dp_res['schema'],
            'order_fields': True
        }
        validate_options.update(goodtables_options)
        report = goodtables.validate(evaluated_rows, **validate_options)

        report_file_path = '{}/{}.json'.format(reports_path, dp_res['name'])
        if write_report:
            os.makedirs(reports_path, exist_ok=True)
            with io.open(report_file_path, 'w') as f:
                f.write(json.dumps(report, indent=4))

        if report['error-count'] > 0 and fail_on_error:
            msg = 'Datapackage resource \'{}\' failed'.format(dp_res['name'])
            msg += ' Goodtables validation.'
            if write_report:
                msg += ' See report for details: {}'.format(report_file_path)
            raise RuntimeError(msg)

        yield from rows

    for i, res in enumerate(res_iter_):
        dp_res = datapackage['resources'][i]

        yield _validate_resource(res, dp_res)


# add report info to datapackage
if write_report:
    reports = datapackage.get('reports', [])
    for dp_res in datapackage['resources']:
        reports.append({
            'resource': dp_res['name'],
            'reportType': 'goodtables',
            'path': '{}/{}.json'.format(reports_path, dp_res['name'])
        })
    datapackage['reports'] = reports


spew(datapackage, process_resources(res_iter, datapackage, goodtables_options))

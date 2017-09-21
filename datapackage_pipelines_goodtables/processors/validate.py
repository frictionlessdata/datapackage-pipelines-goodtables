import os
import io
import json
import itertools

from datapackage_pipelines.wrapper import ingest, spew
import goodtables

import logging
log = logging.getLogger(__name__)


def _log_report(report, fail_on_error=True, suppress_if_valid=False):
    '''
    Log a validation report with a general Dataset summary and warnings, and
    table errors.
    '''

    def _split_json_log(json_log, level=logging.INFO, line_limit=None):
        '''Dumps json to an indented string, splits at new lines, then logs
        each line out to `level`, optionally limiting line length to
        `line_limit`.'''
        json_log_dumps = json.dumps(json_log, indent=4)
        [report_lines.append((level, l[:line_limit]))
         for l in json_log_dumps.splitlines()]

    tables = report.pop('tables')
    warnings = report.pop('warnings')
    report_lines = []

    has_errors = False

    report_lines.append((logging.INFO, 'DATASET'))
    report_lines.append((logging.INFO, '='*7))

    _split_json_log(report)

    if warnings:
        report_lines.append((logging.INFO, '-'*9))
        report_lines.append((logging.INFO, 'Table Warnings:'))
        for warning in warnings:
            _split_json_log(warning, level=logging.WARN, line_limit=128)

    for table_number, table in enumerate(tables, start=1):
        report_lines.append((logging.INFO, 'TABLE [%s]' % table_number))
        report_lines.append((logging.INFO, '='*9))
        errors = table.pop('errors')
        _split_json_log(table, line_limit=128)
        if errors:
            has_errors = True
            report_lines.append((logging.INFO, '-'*9))
            for error in errors:
                error = {key: value or '-' for key, value in error.items()}
                template = '[{row-number},{column-number}] [{code}] {message}'
                message = template.format(**error)
                report_lines.append((logging.ERROR, message))

    if not suppress_if_valid or has_errors:
        [log.log(l[0], l[1]) for l in report_lines]

    if has_errors and fail_on_error:
        raise RuntimeError('Datapackage failed Goodtables validation. '
                           + 'See log for details.')


parameters, datapackage, res_iter = ingest()

fail_on_error = parameters.get('fail_on_error', True)
suppress_if_valid = parameters.get('suppress_if_valid', True)
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

        os.makedirs(reports_path, exist_ok=True)
        with io.open('{}/{}.json'.format(reports_path,
                                         dp_res['name']), 'w') as f:
            f.write(json.dumps(report, indent=4))

        _log_report(report, fail_on_error, suppress_if_valid)

        yield from rows

    for i, res in enumerate(res_iter_):
        dp_res = datapackage['resources'][i]

        yield _validate_resource(res, dp_res)


# add report info to datapackage
reports = datapackage.get('reports', [])
for dp_res in datapackage['resources']:
    reports.append({
        'resource': dp_res['name'],
        'reportType': 'goodtables',
        'path': '{}/{}.json'.format(reports_path, dp_res['name'])
    })
datapackage['reports'] = reports


spew(datapackage, process_resources(res_iter, datapackage, goodtables_options))

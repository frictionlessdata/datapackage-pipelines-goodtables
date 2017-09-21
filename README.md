# Goodtables validation plugin for datapackage-pipelines

[![Travis](https://img.shields.io/travis/frictionlessdata/datapackage-pipelines-goodtables/master.svg)](https://travis-ci.org/frictionlessdata/datapackage-pipelines-goodtables)
[![Coveralls](http://img.shields.io/coveralls/frictionlessdata/datapackage-pipelines-goodtables/master.svg)](https://coveralls.io/r/frictionlessdata/datapackage-pipelines-goodtables?branch=master)
[![PyPi](https://img.shields.io/pypi/v/datapackage-pipelines-goodtables.svg)](https://pypi.python.org/pypi/datapackage-pipelines-goodtables)
[![SemVer](https://img.shields.io/badge/versions-SemVer-brightgreen.svg)](http://semver.org/)
[![Gitter](https://img.shields.io/gitter/room/frictionlessdata/chat.svg)](https://gitter.im/frictionlessdata/chat)


A [datapackage-pipelines](https://github.com/frictionlessdata/datapackage-pipelines) processor to validate tabular resources using [goodtables](https://github.com/frictionlessdata/goodtables-py).


## Install

```
# clone the repo and install it with pip

git clone https://github.com/frictionlessdata/datapackage-pipelines-goodtables.git
pip install -e .
```

## Usage

Add the following to the pipeline-spec.yml configuration to validate each resource in the datapackage. A report is outputted to the logger.

```yaml
  ...
  - run: goodtables.validate
    parameters:
        fail_on_error: True,
        suppress_if_valid: False,
        goodtables:
            <key>: <value>  # options passed to goodtables.validate()
```

- `fail_on_error`: An optional boolean to determine whether the pipeline should fail on validation error (default `True`).
- `suppress_if_valid`: An optional boolean to determine whether the goodtables validation report should be logged if there are no errors (default `False`).
- `goodtables`: An optional object passed to `goodtables.validate()` to customise its behaviour. See [`goodtables.validate()`](https://github.com/frictionlessdata/goodtables-py/#validatesource-options) for available options.

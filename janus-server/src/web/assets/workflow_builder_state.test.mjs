import assert from 'node:assert/strict';
import test from 'node:test';

import {
  buildDerivedJobs,
  createCatalogLookup,
  OutcomePolicy,
  inferBinding,
  outputOptionsFor,
  promotedArtifactSourceOptions,
  readInputBinding,
  serializeJobs,
  validateJobs,
} from './workflow_builder_state.js';

function fieldRow(fields, inputRows = [], outcomePolicy = OutcomePolicy.REQUIRED) {
  return {
    _outcomePolicy: outcomePolicy,
    querySelector(selector) {
      const match = selector.match(/^\[data-field="(.+)"\]$/);
      if (!match) return null;
      return { value: fields[match[1]] || '' };
    },
    querySelectorAll(selector) {
      return selector === '[data-input-row]' ? inputRows : [];
    }
  };
}

function inputRow({ name, kind, mode = 'literal', value = '' }) {
  return {
    dataset: {
      inputName: name,
      inputKind: kind
    },
    querySelector(selector) {
      if (selector === '[data-binding-mode]') return { value: mode };
      if (selector === '[data-binding-value]') return { value };
      return null;
    }
  };
}

const catalog = [
  {
    id: 'runner-1',
    name: 'Linux',
    jobs: [
      {
        name: 'build',
        inputs: {
          source: { type: 'artifact', required: true },
          version: { type: 'string', required: false }
        },
        outputs: {
          app: { type: 'artifact', required: true },
          version: { type: 'string', required: false }
        }
      },
      {
        name: 'deploy',
        outputs: {}
      }
    ]
  }
];

test('catalog lookup exposes runners, jobs, and job definitions', () => {
  const lookup = createCatalogLookup(catalog);

  assert.equal(lookup.getRunner('runner-1').name, 'Linux');
  assert.equal(lookup.getRunner('missing'), null);
  assert.equal(lookup.getRunnerJobs('runner-1').length, 2);
  assert.equal(lookup.getJobDefinition('runner-1', 'build').outputs.app.type, 'artifact');
});

test('buildDerivedJobs reads row fields and computes display names', () => {
  const lookup = createCatalogLookup(catalog);
  const rows = [
    fieldRow({ runner_id: 'runner-1', runner_job_name: 'build' }),
    fieldRow({ runner_id: '', runner_job_name: '' })
  ];

  assert.deepEqual(
    buildDerivedJobs(rows, lookup.getRunner).map((job) => ({
      jobIndex: job.jobIndex,
      runnerId: job.runnerId,
      runnerJobName: job.runnerJobName,
      name: job.name
    })),
    [
      { jobIndex: 0, runnerId: 'runner-1', runnerJobName: 'build', name: 'Linux / build' },
      { jobIndex: 1, runnerId: '', runnerJobName: '', name: 'job-2' }
    ]
  );
});

test('inferBinding maps manifest defaults and saved bindings', () => {
  assert.deepEqual(inferBinding('source', 'artifact', undefined), {
    mode: 'source_artifact',
    value: 'source.tar.gz'
  });
  assert.deepEqual(inferBinding('commit', 'string', undefined), {
    mode: 'commit',
    value: ''
  });
  assert.deepEqual(inferBinding('published', 'boolean', { kind: 'literal', value: true }), {
    mode: 'literal',
    value: 'true'
  });
  assert.deepEqual(inferBinding('version', 'string', {
    kind: 'job_output',
    job_index: 0,
    output_name: 'version'
  }), {
    mode: 'output_value',
    value: '{"kind":"job_output","job_index":0,"output_name":"version"}'
  });
  const promoted = {
    kind: 'promoted_artifact',
    source_runner_id: 'runner-1',
    source_job_name: 'build',
    output_name: 'app'
  };
  assert.deepEqual(inferBinding('app', 'artifact', promoted), {
    mode: 'promoted_artifact',
    value: JSON.stringify(promoted)
  });
});

test('promotedArtifactSourceOptions exposes advertised artifact outputs', () => {
  assert.deepEqual(promotedArtifactSourceOptions(catalog), [{
    value: JSON.stringify({
      kind: 'promoted_artifact',
      source_runner_id: 'runner-1',
      source_job_name: 'build',
      output_name: 'app'
    }),
    label: 'Linux / build / app'
  }]);
});

test('readInputBinding parses literal and special binding modes from fake rows', () => {
  const promoted = {
    kind: 'promoted_artifact',
    source_runner_id: 'runner-1',
    source_job_name: 'build',
    output_name: 'app'
  };
  assert.deepEqual(readInputBinding(inputRow({
    name: 'app',
    kind: 'artifact',
    mode: 'promoted_artifact',
    value: JSON.stringify(promoted)
  })), ['app', promoted]);

  assert.deepEqual(readInputBinding(inputRow({
    name: 'count',
    kind: 'integer',
    value: '42'
  })), ['count', { kind: 'literal', value: 42 }]);

  assert.deepEqual(readInputBinding(inputRow({
    name: 'commit',
    kind: 'string',
    mode: 'commit'
  })), ['commit', { kind: 'commit' }]);

  assert.deepEqual(readInputBinding(inputRow({
    name: 'metadata',
    kind: 'json',
    value: '{"ok":true}'
  })), ['metadata', { kind: 'literal', value: { ok: true } }]);
});

test('readInputBinding handles empty output binding selects', () => {
  assert.deepEqual(readInputBinding(inputRow({
    name: 'source',
    kind: 'artifact',
    mode: 'output_artifact',
    value: ''
  })), ['source', { kind: 'source_artifact' }]);

  assert.deepEqual(readInputBinding(inputRow({
    name: 'version',
    kind: 'string',
    mode: 'output_value',
    value: ''
  })), ['version', { kind: 'literal', value: '' }]);
});

test('serializeJobs filters empty rows and preserves parsed inputs', () => {
  const lookup = createCatalogLookup(catalog);
  const rows = [
    fieldRow(
      { runner_id: 'runner-1', runner_job_name: 'build' },
      [
        inputRow({ name: 'commit', kind: 'string', mode: 'commit' }),
        inputRow({ name: 'source', kind: 'artifact', mode: 'source_artifact' })
      ],
      OutcomePolicy.ALLOWED
    ),
    fieldRow({ runner_id: '', runner_job_name: '' }, [])
  ];
  const jobs = serializeJobs(buildDerivedJobs(rows, lookup.getRunner));

  assert.deepEqual(jobs, [
    {
      runner_id: 'runner-1',
      runner_job_name: 'build',
      inputs: {
        commit: { kind: 'commit' },
        source: { kind: 'source_artifact' }
      },
      outcome_policy: 'allowed_to_fail'
    }
  ]);
});

test('validateJobs reports missing selections and required inputs', () => {
  const lookup = createCatalogLookup(catalog);

  assert.deepEqual(validateJobs([], lookup.getJobDefinition), ['Add at least one job.']);
  assert.deepEqual(
    validateJobs([{ runner_id: '', runner_job_name: '', inputs: {} }], lookup.getJobDefinition),
    ['Job 1: choose a runner.', 'Job 1: choose a job.']
  );
  assert.deepEqual(
    validateJobs([{
      runner_id: 'runner-1',
      runner_job_name: 'build',
      inputs: { source: { kind: 'literal', value: '' } }
    }], lookup.getJobDefinition),
    ['Job 1: set required input source.']
  );
});

test('outputOptionsFor only exposes earlier matching outputs', () => {
  const lookup = createCatalogLookup(catalog);
  const rows = [
    fieldRow({ runner_id: 'runner-1', runner_job_name: 'build' }),
    fieldRow({ runner_id: 'runner-1', runner_job_name: 'deploy' })
  ];
  const derivedJobs = buildDerivedJobs(rows, lookup.getRunner);

  assert.deepEqual(outputOptionsFor(rows[1], derivedJobs, 'artifact', lookup.getJobDefinition), [
    {
      value: '{"kind":"job_output","job_index":0,"output_name":"app"}',
      label: 'app (Linux.build)'
    }
  ]);
  assert.deepEqual(outputOptionsFor(rows[0], derivedJobs, 'artifact', lookup.getJobDefinition), []);
  assert.deepEqual(outputOptionsFor(rows[1], derivedJobs, 'boolean', lookup.getJobDefinition), []);
});

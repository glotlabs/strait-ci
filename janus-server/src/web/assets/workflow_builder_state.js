export const OutcomePolicy = Object.freeze({
  REQUIRED: 'required',
  ALLOWED: 'allowed_to_fail',
});

export const OUTCOME_POLICY_OPTIONS = Object.freeze([
  [OutcomePolicy.REQUIRED, 'Must succeed'],
  [OutcomePolicy.ALLOWED, 'Can fail'],
]);

export function outcomePolicyFromJob(job) {
  if (job.outcome_policy === OutcomePolicy.ALLOWED) return OutcomePolicy.ALLOWED;
  if (job.outcome_policy === OutcomePolicy.REQUIRED) return OutcomePolicy.REQUIRED;
  return OutcomePolicy.REQUIRED;
}

export function createCatalogLookup(catalog) {
  const catalogById = new Map(catalog.map((runner) => [runner.id, runner]));

  function getRunner(runnerId) {
    return catalogById.get(runnerId) || null;
  }

  function getRunnerJobs(runnerId) {
    const runner = getRunner(runnerId);
    return runner ? runner.jobs : [];
  }

  function getJobDefinition(runnerId, jobName) {
    return getRunnerJobs(runnerId).find((job) => job.name === jobName) || null;
  }

  return { getRunner, getRunnerJobs, getJobDefinition };
}

export function buildDerivedJobs(rows, getRunner) {
  return rows.map((row, index) => {
    const runnerId = row.querySelector('[data-field="runner_id"]').value.trim();
    const runnerJobName = row.querySelector('[data-field="runner_job_name"]').value.trim();
    const runner = getRunner(runnerId);
    return {
      row,
      runner,
      jobIndex: index,
      runnerId,
      runnerJobName,
      name: runner && runnerJobName ? `${runner.name} / ${runnerJobName}` : (runnerJobName || `job-${index + 1}`)
    };
  });
}

export function inferBinding(inputName, kind, rawValue) {
  if (kind === 'artifact') {
    if (rawValue && rawValue.kind === 'source_artifact') return { mode: 'source_artifact', value: 'source.tar.gz' };
    if (rawValue && rawValue.kind === 'promoted_artifact') {
      return { mode: 'promoted_artifact', value: JSON.stringify(rawValue) };
    }
    if (rawValue && typeof rawValue === 'object' && rawValue.kind === 'job_output') {
      return { mode: 'output_artifact', value: JSON.stringify(rawValue) };
    }
    if (inputName === 'source') return { mode: 'source_artifact', value: 'source.tar.gz' };
    return { mode: 'source_artifact', value: 'source.tar.gz' };
  }
  if (kind === 'string') {
    if (rawValue && rawValue.kind === 'commit') return { mode: 'commit', value: '' };
    if (rawValue && rawValue.kind === 'branch') return { mode: 'branch', value: '' };
    if (rawValue && typeof rawValue === 'object' && rawValue.kind === 'job_output') {
      return { mode: 'output_value', value: JSON.stringify(rawValue) };
    }
    if (inputName === 'commit') return { mode: 'commit', value: '' };
    if (inputName === 'branch') return { mode: 'branch', value: '' };
    if (rawValue && rawValue.kind === 'literal') return { mode: 'literal', value: typeof rawValue.value === 'string' ? rawValue.value : '' };
    return { mode: 'literal', value: '' };
  }
  if ((kind === 'boolean' || kind === 'integer' || kind === 'json')
    && rawValue && typeof rawValue === 'object'
    && rawValue.kind === 'job_output') {
    return { mode: 'output_value', value: JSON.stringify(rawValue) };
  }
  if (kind === 'boolean') return { mode: 'literal', value: rawValue && rawValue.kind === 'literal' && rawValue.value === true ? 'true' : 'false' };
  if (kind === 'integer') return { mode: 'literal', value: rawValue && rawValue.kind === 'literal' ? String(rawValue.value ?? '') : '' };
  if (kind === 'json') return { mode: 'literal', value: rawValue && rawValue.kind === 'literal' ? JSON.stringify(rawValue.value) : '' };
  return { mode: 'literal', value: rawValue && rawValue.kind === 'literal' ? String(rawValue.value ?? '') : '' };
}

export function readInputBinding(inputRow) {
  const kind = inputRow.dataset.inputKind;
  const name = inputRow.dataset.inputName;
  const modeSelect = inputRow.querySelector('[data-binding-mode]');
  const valueField = inputRow.querySelector('[data-binding-value]');
  const mode = modeSelect ? modeSelect.value : 'literal';
  if (kind === 'artifact') {
    if (mode === 'source_artifact') return [name, { kind: 'source_artifact' }];
    if (mode === 'promoted_artifact') {
      return [name, parseOutputBinding(valueField ? valueField.value : '') || {
        kind: 'promoted_artifact', source_runner_id: '', source_job_name: '', output_name: ''
      }];
    }
    return [name, parseOutputBinding(valueField ? valueField.value : '') || { kind: 'source_artifact' }];
  }
  if (kind === 'string') {
    if (mode === 'commit') return [name, { kind: 'commit' }];
    if (mode === 'branch') return [name, { kind: 'branch' }];
    if (mode === 'output_value') {
      return [name, parseOutputBinding(valueField ? valueField.value : '') || { kind: 'literal', value: '' }];
    }
    return [name, { kind: 'literal', value: valueField ? valueField.value : '' }];
  }
  if (mode === 'output_value') {
    const outputBinding = parseOutputBinding(valueField ? valueField.value : '');
    if (outputBinding) return [name, outputBinding];
  }
  if (kind === 'boolean') {
    return [name, { kind: 'literal', value: valueField.value === 'true' }];
  }
  if (kind === 'integer') {
    const raw = valueField.value.trim();
    const parsed = Number.parseInt(raw, 10);
    return [name, { kind: 'literal', value: Number.isFinite(parsed) ? parsed : raw }];
  }
  if (kind === 'json') {
    const raw = valueField.value.trim();
    if (!raw) return [name, { kind: 'literal', value: {} }];
    try {
      return [name, { kind: 'literal', value: JSON.parse(raw) }];
    } catch (_error) {
      return [name, { kind: 'literal', value: raw }];
    }
  }
  return [name, { kind: 'literal', value: valueField ? valueField.value : '' }];
}

export function serializeJobs(derivedJobs, readBinding = readInputBinding) {
  return derivedJobs.map((job) => {
    const inputsMap = [...job.row.querySelectorAll('[data-input-row]')]
      .map(readBinding)
      .reduce((acc, [key, value]) => {
        acc[key] = value;
        return acc;
      }, {});
    return {
      runner_id: job.runnerId,
      runner_job_name: job.runnerJobName,
      inputs: inputsMap,
      outcome_policy: job.row._outcomePolicy === OutcomePolicy.ALLOWED
        ? OutcomePolicy.ALLOWED
        : OutcomePolicy.REQUIRED
    };
  }).filter((job) => job.runner_id || job.runner_job_name || Object.keys(job.inputs).length > 0);
}

export function validateJobs(jobs, getJobDefinition) {
  const errors = [];
  if (jobs.length === 0) {
    errors.push('Add at least one job.');
    return errors;
  }

  jobs.forEach((job, index) => {
    const label = `Job ${index + 1}`;
    if (!job.runner_id) {
      errors.push(`${label}: choose a runner.`);
    }
    if (!job.runner_job_name) {
      errors.push(`${label}: choose a job.`);
    }
    if (!job.runner_id || !job.runner_job_name) return;

    const definition = getJobDefinition(job.runner_id, job.runner_job_name);
    if (!definition) {
      errors.push(`${label}: selected job is not available for this runner.`);
      return;
    }

    for (const [inputName, inputDef] of Object.entries(definition.inputs || {})) {
      const binding = job.inputs[inputName];
      if (inputDef.required && bindingIsMissing(binding)) {
        errors.push(`${label}: set required input ${inputName}.`);
      }
    }
  });
  return errors;
}

function bindingIsMissing(binding) {
  if (!binding) return true;
  if (binding.kind === 'literal') {
    const value = binding.value;
    if (value === null || value === undefined) return true;
    if (typeof value === 'string') return value.trim() === '';
    return false;
  }
  if (binding.kind === 'job_output') {
    return binding.output_name === '';
  }
  if (binding.kind === 'promoted_artifact') {
    return !binding.source_runner_id || !binding.source_job_name || !binding.output_name;
  }
  return false;
}

export function outputOptionsFor(currentRow, derivedJobs, expectedKind, getJobDefinition) {
  const options = [];
  const currentIndex = derivedJobs.findIndex((job) => job.row === currentRow);
  for (const job of derivedJobs) {
    if (job.row === currentRow) continue;
    if (currentIndex !== -1 && derivedJobs.indexOf(job) >= currentIndex) continue;
    const definition = getJobDefinition(job.runnerId, job.runnerJobName);
    const outputs = definition ? Object.entries(definition.outputs || {}) : [];
    for (const [outputName, outputDef] of outputs) {
      if ((outputDef.type || '') !== expectedKind) continue;
      options.push({
        value: JSON.stringify({ kind: 'job_output', job_index: job.jobIndex, output_name: outputName }),
        label: `${outputName} (${outputJobLabel(job)})`
      });
    }
  }
  return options;
}

function outputJobLabel(job) {
  const runnerName = job.runner?.name || job.runnerId || 'runner';
  const jobName = job.runnerJobName || job.name || `job-${job.jobIndex + 1}`;
  return `${runnerName}.${jobName}`;
}

export function parseOutputBinding(value) {
  if (!value) return null;
  try {
    return JSON.parse(value);
  } catch (_error) {
    return null;
  }
}

export function findDerivedJobByIndex(derivedJobs, jobIndex) {
  return derivedJobs.find((job) => job.jobIndex === jobIndex) || null;
}

export function literalHintFor(kind, mode) {
  if (mode !== 'literal') return '';
  if (kind === 'string') return 'Enter a plain string value.';
  if (kind === 'integer') return 'Enter a signed integer like 42.';
  if (kind === 'boolean') return 'Choose true or false.';
  if (kind === 'json') return 'Enter valid non-null JSON.';
  return '';
}

export function bindingModesFor(kind) {
  if (kind === 'artifact') return [
    ['source_artifact', 'Source archive'],
    ['output_artifact', 'Earlier job output'],
    ['promoted_artifact', 'Artifact selected at run time']
  ];
  if (kind === 'string') return [
    ['literal', 'Literal'],
    ['output_value', 'Job output'],
    ['commit', 'Current commit'],
    ['branch', 'Current branch']
  ];
  if (kind === 'integer' || kind === 'boolean' || kind === 'json') return [
    ['literal', 'Literal'],
    ['output_value', 'Job output']
  ];
  return [['literal', 'Literal']];
}

export function promotedArtifactSourceOptions(catalog) {
  const options = [];
  for (const runner of catalog) {
    for (const job of runner.jobs || []) {
      for (const [outputName, output] of Object.entries(job.outputs || {})) {
        if (output.type !== 'artifact') continue;
        options.push({
          value: JSON.stringify({
            kind: 'promoted_artifact',
            source_runner_id: runner.id,
            source_job_name: job.name,
            output_name: outputName,
          }),
          label: `${runner.name} / ${job.name} / ${outputName}`,
        });
      }
    }
  }
  return options;
}

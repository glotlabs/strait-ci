import {
  makeInput,
  makeSelect,
  replaceOptions,
} from './workflow_builder_dom.js';
import {
  findDerivedJobByIndex,
  outputOptionsFor,
  parseOutputBinding,
} from './workflow_builder_state.js';

export function outputBindingHint({
  row,
  inputRow,
  mode,
  derivedJobs,
  getJobDefinition,
}) {
  if (mode !== 'output_artifact' && mode !== 'output_value') return '';
  const reference = inputRow.dataset.bindingValue || '';
  const kind = inputRow.dataset.inputKind;
  const expectedKind = mode === 'output_artifact' ? 'artifact' : kind;
  const options = outputOptionsFor(row, derivedJobs, expectedKind, getJobDefinition);
  if (options.length === 0) {
    return 'No outputs available';
  }
  if (!reference) return `Select a ${expectedKind} output from an earlier job.`;
  const binding = parseOutputBinding(reference);
  const sourceJob = binding ? findDerivedJobByIndex(derivedJobs, binding.job_index) : null;
  return sourceJob ? `Binding to ${sourceJob.name}.` : '';
}

export function renderOutputBindingOptions({ derivedJobs, getJobDefinition }) {
  for (const job of derivedJobs) {
    for (const inputRow of job.row.querySelectorAll('[data-input-row]')) {
      const mode = inputRow.querySelector('[data-binding-mode]').value;
      const valueField = inputRow.querySelector('[data-binding-value]');
      const kind = inputRow.dataset.inputKind;
      const isOutputBinding = mode === 'output_artifact' || mode === 'output_value';
      if (!valueField || valueField.tagName !== 'SELECT' || !isOutputBinding) continue;

      const selected = inputRow.dataset.bindingValue || valueField.value;
      const expectedKind = mode === 'output_artifact' ? 'artifact' : kind;
      const options = outputOptionsFor(job.row, derivedJobs, expectedKind, getJobDefinition);
      replaceOptions(valueField, options, selected, {
        placeholder: options.length === 0
          ? { label: 'No outputs available', selected: true }
          : null,
        selectFirst: true
      });
      valueField.hidden = options.length === 0;
      inputRow.dataset.bindingValue = valueField.value;
    }
  }
}

export function buildValueField(kind, binding, row, promotedArtifactOptions = []) {
  const field = valueFieldFor(kind, binding, promotedArtifactOptions);
  if (field.bindingValue !== undefined) {
    row.dataset.bindingValue = field.bindingValue;
  }
  return field.element;
}

function valueFieldFor(kind, binding, promotedArtifactOptions) {
  if (kind === 'artifact') {
    const select = makeSelect();
    select.setAttribute('data-binding-value', 'true');
    if (binding.mode === 'source_artifact') {
      replaceOptions(select, [{ value: 'source.tar.gz', label: 'source.tar.gz' }], 'source.tar.gz');
    } else if (binding.mode === 'promoted_artifact') {
      replaceOptions(select, promotedArtifactOptions, binding.value || '', {
        placeholder: promotedArtifactOptions.length === 0
          ? { label: 'No artifact outputs advertised' }
          : { label: 'Choose build artifact source' },
      });
    }
    return { element: select, bindingValue: binding.value || '' };
  }
  if (binding.mode === 'output_value') {
    const select = makeSelect();
    select.setAttribute('data-binding-value', 'true');
    return { element: select, bindingValue: binding.value || '' };
  }
  if (kind === 'string' && binding.mode !== 'literal') {
    const note = document.createElement('div');
    note.className = 'muted';
    note.textContent = binding.mode === 'commit' ? '<commit>' : '<branch>';
    return { element: note };
  }
  if (kind === 'boolean') {
    const select = makeSelect();
    select.setAttribute('data-binding-value', 'true');
    replaceOptions(
      select,
      ['true', 'false'].map((value) => ({ value, label: value })),
      String(binding.value || 'false')
    );
    return { element: select };
  }
  if (kind === 'json') {
    const textarea = document.createElement('textarea');
    textarea.rows = 3;
    textarea.value = binding.value || '';
    textarea.setAttribute('data-binding-value', 'true');
    return { element: textarea };
  }
  const input = makeInput(kind === 'integer' ? 'number' : 'text', binding.value || '');
  input.setAttribute('data-binding-value', 'true');
  return { element: input };
}

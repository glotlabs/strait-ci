import {
  cloneTemplate,
  makeSelect,
  replaceOptions,
} from './workflow_builder_dom.js';
import {
  buildValueField,
  outputBindingHint,
} from './workflow_builder_bindings.js';
import {
  bindingModesFor,
  inferBinding,
  literalHintFor,
  outputOptionsFor,
  promotedArtifactSourceOptions,
} from './workflow_builder_state.js';

export function renderInputTable({
  row,
  derivedJobs,
  getJobDefinition,
  catalog,
  templates,
  onBindingChanged,
}) {
  const wrap = row.querySelector('[data-inputs-wrap]');
  const summary = row.querySelector('[data-input-summary]');
  const runnerId = row.querySelector('[data-field="runner_id"]').value;
  const runnerJobName = row.querySelector('[data-field="runner_job_name"]').value;
  const definition = getJobDefinition(runnerId, runnerJobName);
  const inputs = definition ? Object.entries(definition.inputs || {}) : [];

  wrap.replaceChildren();
  if (inputs.length === 0) {
    wrap.appendChild(cloneTemplate(templates.inputsEmpty));
    if (summary) summary.textContent = 'None';
    return;
  }

  if (summary) summary.textContent = `${inputs.length} input${inputs.length === 1 ? '' : 's'}`;
  const table = cloneTemplate(templates.inputsTable);
  const tbody = table.querySelector('[data-table-body]');
  for (const input of inputs.map(([inputName, inputDef]) => ({ inputName, inputDef }))) {
    tbody.appendChild(renderInputRow({
      row,
      input,
      derivedJobs,
      getJobDefinition,
      catalog,
      onBindingChanged,
    }));
  }
  wrap.appendChild(table);
}

export function renderOutputTable({ derivedJobs, getJobDefinition, templates }) {
  for (const job of derivedJobs) {
    const wrap = job.row.querySelector('[data-outputs-wrap]');
    const summary = job.row.querySelector('[data-output-summary]');
    if (!wrap) continue;

    const definition = getJobDefinition(job.runnerId, job.runnerJobName);
    const outputs = definition ? Object.entries(definition.outputs || {}) : [];
    wrap.replaceChildren();
    if (summary) summary.textContent = outputs.length === 0
      ? 'None'
      : `${outputs.length} output${outputs.length === 1 ? '' : 's'}`;

    if (outputs.length === 0) {
      wrap.appendChild(cloneTemplate(templates.outputsEmpty));
      continue;
    }

    const table = cloneTemplate(templates.outputsTable);
    const tbody = table.querySelector('[data-table-body]');
    for (const [outputName, outputDef] of outputs) {
      tbody.appendChild(renderOutputRow(outputName, outputDef));
    }
    wrap.appendChild(table);
  }
}

function renderInputRow({
  row,
  input,
  derivedJobs,
  getJobDefinition,
  catalog = [],
  onBindingChanged,
}) {
  const { inputName, inputDef } = input;
  const inputRow = document.createElement('tr');
  inputRow.setAttribute('data-input-row', 'true');
  inputRow.dataset.inputName = inputName;
  inputRow.dataset.inputKind = inputDef.type;
  const binding = inferBinding(inputName, inputDef.type, row._inputs[inputName]);
  inputRow.dataset.bindingValue = binding.value || '';

  const nameCell = inputNameCell(inputName, Boolean(inputDef.required));
  const typeCell = textCell(inputDef.type);
  const modeCell = document.createElement('td');
  const modeSelect = makeSelect();
  modeSelect.setAttribute('data-binding-mode', 'true');
  replaceOptions(
    modeSelect,
    bindingModesFor(inputDef.type).map(([value, label]) => ({ value, label })),
    binding.mode
  );
  modeCell.appendChild(modeSelect);
  const valueCell = document.createElement('td');

  const paintValueField = () => {
    valueCell.replaceChildren();
    const isOutputBinding = modeSelect.value === 'output_artifact' || modeSelect.value === 'output_value';
    const expectedOutputKind = modeSelect.value === 'output_artifact' ? 'artifact' : inputDef.type;
    const currentBinding = {
      mode: modeSelect.value,
      value: inputRow.dataset.bindingValue || binding.value || ''
    };
    const field = buildValueField(
      inputDef.type,
      currentBinding,
      inputRow,
      promotedArtifactSourceOptions(catalog || [])
    );
    if (isOutputBinding && outputOptionsFor(row, derivedJobs, expectedOutputKind, getJobDefinition).length === 0) {
      field.hidden = true;
    }
    if ('value' in field) {
      field.addEventListener('input', () => {
        inputRow.dataset.bindingValue = field.value || '';
        onBindingChanged();
      });
      field.addEventListener('change', () => {
        inputRow.dataset.bindingValue = field.value || '';
        onBindingChanged();
      });
    }
    valueCell.appendChild(field);

    const hint = literalHintFor(inputDef.type, modeSelect.value)
      || outputBindingHint({
        row,
        inputRow,
        mode: modeSelect.value,
        derivedJobs,
        getJobDefinition
      })
      || (modeSelect.value === 'promoted_artifact'
        ? 'Choose the build output; the operator selects one of its 10 newest successful artifacts when starting this workflow.'
        : '');
    if (hint) {
      const note = document.createElement('div');
      note.className = 'muted';
      note.textContent = hint;
      valueCell.appendChild(note);
    }
  };

  modeSelect.addEventListener('change', () => {
    inputRow.dataset.bindingValue = '';
    paintValueField();
    onBindingChanged();
  });
  paintValueField();

  inputRow.append(nameCell, typeCell, modeCell, valueCell);
  return inputRow;
}

function renderOutputRow(outputName, outputDef) {
  const outputRow = document.createElement('tr');
  const nameCell = document.createElement('td');
  const outputStrong = document.createElement('strong');
  outputStrong.textContent = outputName;
  nameCell.appendChild(outputStrong);
  outputRow.append(
    nameCell,
    textCell(outputDef.type || 'unknown'),
    textCell(outputDef.required ? 'required' : 'optional')
  );
  return outputRow;
}

function inputNameCell(inputName, required) {
  const nameCell = document.createElement('td');
  const nameStrong = document.createElement('strong');
  nameStrong.textContent = inputName;
  nameCell.appendChild(nameStrong);
  if (required) {
    const requiredBadge = document.createElement('span');
    requiredBadge.className = 'badge badge-warning';
    requiredBadge.textContent = 'required';
    nameCell.append(' ', requiredBadge);
  }
  return nameCell;
}

function textCell(text) {
  const cell = document.createElement('td');
  cell.textContent = text;
  return cell;
}

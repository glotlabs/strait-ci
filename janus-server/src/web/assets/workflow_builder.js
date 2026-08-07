import {
  renderOutputBindingOptions,
} from '/assets/workflow_builder_bindings.js';
import { addWorkflowRow } from '/assets/workflow_builder_rows.js';
import {
  renderInputTable as renderWorkflowInputTable,
  renderOutputTable as renderWorkflowOutputTable,
} from '/assets/workflow_builder_tables.js';
import {
  buildDerivedJobs,
  createCatalogLookup,
  serializeJobs,
  validateJobs,
} from '/assets/workflow_builder_state.js';

(() => {
  const list = document.getElementById('workflow-job-list');
  const addButton = document.getElementById('workflow-add-job');
  const jobsJsonField = document.getElementById('workflow-jobs-json');
  const form = jobsJsonField.closest('form');
  const validationErrors = document.getElementById('workflow-validation-errors');
  const jobRowTemplate = document.getElementById('workflow-job-row-template');
  const inputsEmptyTemplate = document.getElementById('workflow-inputs-empty-template');
  const outputsEmptyTemplate = document.getElementById('workflow-outputs-empty-template');
  const inputsTableTemplate = document.getElementById('workflow-inputs-table-template');
  const outputsTableTemplate = document.getElementById('workflow-outputs-table-template');
  const catalog = JSON.parse(document.getElementById('workflow-runner-catalog').textContent || '[]');
  const initialJobs = JSON.parse(document.getElementById('workflow-initial-jobs').textContent || '[]');
  const { getRunner, getRunnerJobs, getJobDefinition } = createCatalogLookup(catalog);
  const templates = {
    inputsEmpty: inputsEmptyTemplate,
    outputsEmpty: outputsEmptyTemplate,
    inputsTable: inputsTableTemplate,
    outputsTable: outputsTableTemplate
  };

  function syncJobsJson() {
    const derivedJobs = currentDerivedJobs();
    renderOutputBindingOptions({ derivedJobs, getJobDefinition });
    jobsJsonField.value = JSON.stringify(serializeJobs(derivedJobs));
    renderOutputTable(derivedJobs);
    clearValidationErrors();
  }

  function currentSerializedJobs() {
    return serializeJobs(currentDerivedJobs());
  }

  function clearValidationErrors() {
    if (!validationErrors) return;
    validationErrors.hidden = true;
    validationErrors.textContent = '';
  }

  function showValidationErrors(errors) {
    if (!validationErrors) return;
    validationErrors.hidden = errors.length === 0;
    validationErrors.textContent = errors.join(' ');
  }

  function currentDerivedJobs() {
    return buildDerivedJobs([...list.querySelectorAll('[data-workflow-job-row]')], getRunner);
  }

  function renderInputTable(row) {
    renderWorkflowInputTable({
      row,
      derivedJobs: currentDerivedJobs(),
      getJobDefinition,
      catalog,
      templates,
      onBindingChanged: syncJobsJson
    });
  }

  function renderOutputTable(derivedJobs) {
    renderWorkflowOutputTable({ derivedJobs, getJobDefinition, templates });
  }

  function addRow(job) {
    addWorkflowRow({
      job,
      catalog,
      list,
      jobRowTemplate,
      getRunner,
      getRunnerJobs,
      renderInputTable,
      renderOutputTable,
      syncJobsJson,
    });
  }

  addButton.addEventListener('click', () => addRow({
    runner_id: catalog.length === 1 ? catalog[0].id : '',
    runner_job_name: '',
    inputs: {}
  }));

  for (const job of initialJobs) {
    addRow(job);
  }

  if (form) {
    form.addEventListener('submit', (event) => {
      syncJobsJson();
      const errors = validateJobs(currentSerializedJobs(), getJobDefinition);
      if (errors.length === 0) return;
      event.preventDefault();
      showValidationErrors(errors);
      validationErrors?.focus?.();
    });
  }
})();

// Concede lectura sobre el mart de retención al principal definido en
// workflow_settings.yaml (vars.reader_principal). Si la variable está vacía,
// la acción se compila deshabilitada para que el pipeline funcione en cualquier proyecto.
const readerPrincipal = dataform.projectConfig.vars.reader_principal;

operate("grant_permissions", {
  tags: ["security"],
  disabled: !readerPrincipal
}).queries(ctx => `
  GRANT \`roles/bigquery.dataViewer\`
  ON TABLE ${ctx.ref("fact_cohort_retention")}
  TO "${readerPrincipal}"
`);

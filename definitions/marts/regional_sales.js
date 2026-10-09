const constants = require("includes/constants.js");

constants.countries.forEach(country => {
    publish(`sales_${country.code.toLowerCase()}`, {
        type: country.table_type,
        schema: "dataform_marts",
        tags: ["regional_reports"]
    }).query(ctx => `
    SELECT
      order_id,
      user_id,
      sale_price,
      sale_price * ${country.tax} as tax_amount,
      sale_price * (1 + ${country.tax}) as total_amount_with_tax,
      '${country.code}' as region_code
    FROM
      ${ctx.ref("orders_enriched")}
    WHERE
      country = '${country.code}'
  `);
});
# Published analytics regression fixtures

`published_analytics_v1.json` freezes representative semantic outputs consumed
by the dashboard and Stock Research views. It deliberately uses synthetic prices
and evidence; live EOD values, dates and universe membership are not frozen.

Change an expected result only when the corresponding published contract is
intentionally revised. A semantic revision must also bump the affected production
contract version and this fixture's contract name so result drift cannot be
accepted as an incidental test update.

`portfolio_release_v1.json` freezes a representative reconciled two-stock
portfolio across the overview, cash-flow-neutral performance and risk-analysis
contracts. Its synthetic sessions include an interim drawdown, an external
contribution, cash income and a cash fee. Volatile database identifiers and
timestamps are deliberately excluded; the fixture protects semantic outputs,
gates and the local-only/no-ordering boundary.

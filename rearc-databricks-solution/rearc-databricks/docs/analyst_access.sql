-- OPTIONAL TEMPLATE ONLY. Replace the group name with an existing approved group.
-- Execute as a workspace/catalog administrator only when ready to grant that access.
-- This file is not included in the automated workflow.
GRANT USE CATALOG ON CATALOG workspace TO `YOUR_ANALYST_GROUP`;
GRANT USE SCHEMA ON SCHEMA workspace.rearc_quest TO `YOUR_ANALYST_GROUP`;
GRANT SELECT ON TABLE workspace.rearc_quest.gold_population_stats TO `YOUR_ANALYST_GROUP`;
GRANT SELECT ON TABLE workspace.rearc_quest.gold_best_year TO `YOUR_ANALYST_GROUP`;
GRANT SELECT ON TABLE workspace.rearc_quest.gold_series_population TO `YOUR_ANALYST_GROUP`;
-- Do not grant READ VOLUME or schema-wide SELECT merely to consume Gold.

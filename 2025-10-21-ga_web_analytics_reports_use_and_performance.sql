CREATE OR REPLACE TABLE Row.ga_web_analytics_reports_use_and_performance AS 

SELECT DISTINCT
  -- DEFININDO AS COLUNAS (date, session_id, user_id, user_type, user_school, view_home, view_report_use, view_report_performance)
  FORMAT_DATE('%d%m%Y', PARSE_DATE('%Y%m%d', event_date)) AS date,
  (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id,
  user_id,
  (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'user_type') AS user_type,
  (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'user_school') AS user_school,
  MAX(CASE WHEN event_name = 'page_view' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%/home%' THEN TRUE ELSE FALSE END) AS view_home,
  MAX(CASE WHEN event_name = 'page_view' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%/ionica-platform-fe/reports?type=use%' THEN TRUE ELSE FALSE END) AS view_report_use,
  MAX(CASE WHEN event_name = 'page_view' AND (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'page_location') LIKE '%/ionica-platform-fe/reports?type=performance%' THEN TRUE ELSE FALSE END) AS view_report_performance



FROM
  `analytics-bigquery-321918.analytics_265380081.events_*`

WHERE
  -- FILTROS (data inicial pelo suffix da events_ e user_id not null)
  _TABLE_SUFFIX >= '20250901'
  AND user_id IS NOT NULL

GROUP BY
  -- ORGANIZANDO AS COLUNAS
  1, 2, 3, 4, 5

ORDER BY
  -- ORDENANDO POR DATA ASC E session_id ASC
  date ASC, session_id ASC

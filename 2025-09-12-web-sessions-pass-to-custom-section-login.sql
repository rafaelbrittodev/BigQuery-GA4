--- Contagem de sessões que passaram pelo fluxo de login Iônica WEB (custom_section = login)
SELECT 
  FORMAT_TIMESTAMP('%Y-%m-%d %H:%M', TIMESTAMP_MICROS(event_timestamp), 'UTC-3') AS datetime_br,
  (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'custom_section' AND value.string_value = 'login') AS login,
  COUNT(DISTINCT (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS count_session
FROM `analytics-bigquery-321918.analytics_265380081.events_*`
WHERE (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'custom_section' AND value.string_value = 'login') IS NOT NULL 
GROUP BY 1, 2
ORDER BY 1 DESC
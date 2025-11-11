--- Consulta Programada p.2 - consulta-programada-trusted-ga-web-analytics-login
CREATE OR REPLACE TABLE Trusted.ga_web_analytics_login_daily AS 

WITH 
 base_real_time AS (
  SELECT 
    FORMAT_TIMESTAMP('%Y-%m-%d',TIMESTAMP_MICROS(event_timestamp), 'UTC-3') AS realtime_br,
    (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'custom_section' AND value.string_value = 'login') AS login,
    COUNT(DISTINCT (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id')) AS count_session
  FROM `analytics-bigquery-321918.analytics_265380081.events_intraday_*`
    WHERE (SELECT value.string_value FROM UNNEST(event_params) WHERE key = 'custom_section' AND value.string_value = 'login') IS NOT NULL 
    GROUP BY 1,2
    ORDER BY 1 DESC 
)

  SELECT 
    realtime_br,
    count_session,
    media_movel_7d,
    media_movel_3m,
    media_movel_6m,
    CASE WHEN count_session > media_movel_7d THEN TRUE
    WHEN count_session > media_movel_3m THEN TRUE
    WHEN count_session > media_movel_6m THEN TRUE ELSE FALSE END AS validador,
  FROM base_real_time
  LEFT JOIN sandbox.ga_web_analytics_login AS media_movel
  ON media_movel.datetime_br = base_real_time.realtime_br



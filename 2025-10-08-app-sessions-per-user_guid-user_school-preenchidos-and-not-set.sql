WITH daily_sessions AS (
    -- 1. Base: Extrai os dados necessários e atribui status (preenchido/not set) para user_guid e user_school
    SELECT
        PARSE_DATE('%Y%m%d', event_date) AS event_date,
        (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'status_login') AS status_login,
        CASE 
            WHEN (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'user_guid') IS NULL THEN '(not set)'
            ELSE 'preenchido'
        END AS user_guid_status,
        CASE 
            WHEN (SELECT value.string_value FROM UNNEST(user_properties) WHERE key = 'user_school') IS NULL THEN '(not set)'
            ELSE 'preenchido'
        END AS user_school_status,
        (SELECT value.int_value FROM UNNEST(event_params) WHERE key = 'ga_session_id') AS session_id
    FROM 
        `analytics-bigquery-321918.analytics_394562882.events_*`
    WHERE 
        -- Filtra pelo período desejado
        _TABLE_SUFFIX >= '20250709'
),
daily_metrics AS (
    -- 2. Agregação: Conta as sessões por dia para cada uma das 4 categorias de status
    SELECT
        event_date,
        -- Total de sessões para referência
        COUNT(DISTINCT session_id) AS sessions_total_day,
        -- Contagens específicas de user_guid
        COUNT(DISTINCT CASE WHEN user_guid_status = 'preenchido' THEN session_id END) AS sessions_user_guid_preenchido,
        COUNT(DISTINCT CASE WHEN user_guid_status = '(not set)' THEN session_id END) AS sessions_user_guid_not_set,
        -- Contagens específicas de user_school
        COUNT(DISTINCT CASE WHEN user_school_status = 'preenchido' THEN session_id END) AS sessions_user_school_preenchido,
        COUNT(DISTINCT CASE WHEN user_school_status = '(not set)' THEN session_id END) AS sessions_user_school_not_set
    FROM 
        daily_sessions
    WHERE 
        status_login = 'logado'
    GROUP BY 1
)
-- 3. Resultado Final: Adiciona a numeração dos dias (0, 1, 2, ...) e organiza a saída
SELECT
    ROW_NUMBER() OVER (ORDER BY event_date) - 1 AS day_number, -- Numeração de 0 a N-1
    event_date,
    sessions_total_day,
    sessions_user_guid_preenchido,
    sessions_user_guid_not_set,
    sessions_user_school_preenchido,
    sessions_user_school_not_set
FROM 
    daily_metrics
ORDER BY 
    event_date ASC
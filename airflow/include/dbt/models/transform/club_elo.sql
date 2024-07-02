
with avg_p1 as (
  SELECT
    CAST(AVG(DATE_DIFF(date(date_of_birth), DATE('1970-01-01'), DAY)) as integer ) AS avg_p1_days,
  FROM
      {{ source('apc_dwh', 'raw_players1') }}
  WHERE
      date_of_birth IS NOT NULL
),
avg_p2 as (
  SELECT
    CAST(AVG(DATE_DIFF(date(date_of_birth), DATE('1970-01-01'), DAY)) as integer ) AS avg_p2_days,
  FROM
      {{ source('apc_dwh', 'raw_players2') }}
  WHERE
      date_of_birth IS NOT NULL
), 
avg_p3 as (
  SELECT
    CAST(AVG(DATE_DIFF(date(date_of_birth), DATE('1970-01-01'), DAY)) as integer ) AS avg_p3_days,
  FROM
      {{ source('apc_dwh', 'raw_players3') }}
  WHERE
      date_of_birth IS NOT NULL
), 
clean_data_player1 as (
  SELECT 
    FIRST_VALUE(provider_id IGNORE NULLS) over (order by provider_id) as provider_id
    , COALESCE(player_name, concat(first_name, " ", last_name)) as name
    , CASE
      WHEN date_of_birth IS NULL 
      THEN 
        DATE_ADD(DATE('1970-01-01'), INTERVAL avg_p1.avg_p1_days DAY)
      ELSE 
        date(date_of_birth)
    END AS dob
    , gender
    , country
  FROM 
    {{ source('apc_dwh', 'raw_players1') }} as p1
    , avg_p1
), 
clean_data_player2 as (
  SELECT 
    FIRST_VALUE(provider_id IGNORE NULLS) over (order by provider_id) as provider_id
    , COALESCE(player_name, concat(first_name, " ", last_name)) as name
    , CASE
      WHEN date_of_birth IS NULL 
      THEN 
        DATE_ADD(DATE('1970-01-01'), INTERVAL avg_p2.avg_p2_days DAY)
      ELSE 
        date(date_of_birth)
    END AS dob
    , gender
    , country
  FROM 
    {{ source('apc_dwh', 'raw_players2') }} as p2
    , avg_p2
), 
clean_data_player3 as (
  SELECT 
    FIRST_VALUE(provider_id IGNORE NULLS) over (order by provider_id) as provider_id
    , COALESCE(player_name, concat(first_name, " ", last_name)) as name
    , CASE
      WHEN date_of_birth IS NULL 
      THEN 
        DATE_ADD(DATE('1970-01-01'), INTERVAL avg_p3.avg_p3_days DAY)
      ELSE 
        date(date_of_birth)
    END AS dob
    , gender
    , country
  FROM 
    {{ source('apc_dwh', 'raw_players3') }} as p3
    , avg_p3
),
join_p1_p2 as (
  SELECT
    COALESCE(d1.name, d2.name) AS name,
    COALESCE(d1.dob, d2.dob) AS dob,
    COALESCE(d1.gender, d2.gender) AS gender,
    COALESCE(d1.country, d2.country) AS country,
    d1.provider_id AS provider_id1,
    d2.provider_id AS provider_id2,
    concat(d1.name," vs " , d2.name) as similar
  FROM
    clean_data_player1 as d1
  FULL OUTER JOIN
    clean_data_player2 as d2
  ON
    bqutil.fn.levenshtein(d1.name, d2.name) < 5 AND d1.country = d2.country AND d1.gender = d2.gender
), 
players_merge as (
  SELECT 
    COALESCE(d1d2.name, d3.name) AS name,
    COALESCE(d1d2.dob, d3.dob) AS dob,
    COALESCE(d1d2.gender, d3.gender) AS gender,
    COALESCE(d1d2.country, d3.country) AS country,
    d1d2.provider_id1,
    d1d2.provider_id2,
    d3.provider_id AS provider_id3
    , d1d2.similar
    , concat(d1d2.name," vs " , d3.name) as similar3
  FROM 
    join_p1_p2 AS d1d2
  FULL OUTER JOIN
    clean_data_player3 AS d3
  ON
    bqutil.fn.levenshtein(d1d2.name, d3.name) < 5 AND d1d2.country = d3.country AND d1d2.gender = d3.gender
)
select
  {{ dbt_utils.generate_surrogate_key(['name']) }} as global_player_id,
  name
  , dob
  , provider_id1
  , provider_id2
  , provider_id3
from (
  select 
  p.*,
  row_number() over(partition by p.name) as position
  from players_merge as p
)
where  position = 1
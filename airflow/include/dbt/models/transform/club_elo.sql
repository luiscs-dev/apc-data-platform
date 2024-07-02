
SELECT
  `Rank` as `rank`
  , Club as club_name
  , Country as country
  , `Level` as `level`
  , Elo as elo
  , date(`From`) as date_from
  , date(`To`) as date_to
FROM
    {{ source('apc_dwh', 'raw_club_elo') }}

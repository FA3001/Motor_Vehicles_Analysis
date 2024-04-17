{{ config(materialized='table') }}

(SELECT * FROM "magic".mvc_sum_2020) UNION ALL
(SELECT * FROM "magic".mvc_sum_2021) UNION ALL
(SELECT * FROM "magic".mvc_sum_2022) 
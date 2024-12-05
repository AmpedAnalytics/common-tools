-- Custom parameters for the price period
DECLARE @start_date AS DATETIME = ?;

-- Make a table combining price and demand data
WITH price_demand_table AS (
    SELECT
        price_table.REGIONID as Region,
        price_table.SETTLEMENTDATE as DateStamp,
        price_table.RRP as Price,
        demand_table.TOTALDEMAND as Demand

    FROM
       (SELECT
           prices.SETTLEMENTDATE,
           prices.RRP,
           prices.REGIONID

        FROM
           DISPATCHPRICE prices
    
        -- Filter prices during market interventions
        WHERE
            prices.SETTLEMENTDATE >= @start_date
            AND prices.INTERVENTION = 0) AS price_table
    
    INNER JOIN
       (SELECT
            demand.SETTLEMENTDATE,
            demand.TOTALDEMAND,
            demand.REGIONID

        FROM
            DISPATCHREGIONSUM demand         
         
        -- Filter demand during market interventions
        INNER JOIN
            DISPATCHCASESOLUTION solution
                ON demand.SETTLEMENTDATE = solution.SETTLEMENTDATE
                AND demand.INTERVENTION = solution.INTERVENTION

        WHERE
            demand.SETTLEMENTDATE >= @start_date) AS demand_table
    
    ON
        price_table.SETTLEMENTDATE = demand_table.SETTLEMENTDATE
        AND price_table.REGIONID = demand_table.REGIONID
)

-- Get price and demand for each region
SELECT
    DateStamp,
    Region,
    Demand,
    Price
FROM
    price_demand_table;

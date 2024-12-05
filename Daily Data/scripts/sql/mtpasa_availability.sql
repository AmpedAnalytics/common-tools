WITH LatestOffers AS (
    SELECT 
        DAY,
        DUID,
        PASAAVAILABILITY,
        LATEST_OFFER_DATETIME,
        ROW_NUMBER() OVER (PARTITION BY DAY, DUID ORDER BY LATEST_OFFER_DATETIME DESC) AS rn
    FROM 
        MTPASA_DUIDAVAILABILITY
    WHERE 
        DUID IN ('{duid_list}')
    AND 
        DAY BETWEEN '{start_date}' AND '{end_date}'
)
SELECT 
    DAY,
    DUID,
    PASAAVAILABILITY
FROM 
    LatestOffers
WHERE 
    rn = 1
ORDER BY 
    DAY, DUID;
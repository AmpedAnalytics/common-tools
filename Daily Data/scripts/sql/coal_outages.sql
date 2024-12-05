
DECLARE @now AS DATETIME = '{day}'
DECLARE @24_hour_ago AS DATETIME = DATEADD(HOUR, -24, @now)
DECLARE @today AS DATETIME = DATETRUNC(DAY,@now)
DECLARE @1000_day_ago AS DATETIME = DATEADD(DAY, -1000, @today)
DECLARE @12_day_ago AS DATETIME = DATEADD(DAY, -12, @today)
DECLARE @mw_threshold AS INT = 5;


IF OBJECT_ID(N'tempdb..#duids') IS NOT NULL
BEGIN
DROP TABLE #duids;
END;

IF OBJECT_ID(N'tempdb..#gi') IS NOT NULL
BEGIN
DROP TABLE #gi;
END;

IF OBJECT_ID(N'tempdb..#dot') IS NOT NULL
BEGIN
DROP TABLE #dot;
END;

IF OBJECT_ID(N'tempdb..#oid') IS NOT NULL
BEGIN
DROP TABLE #oid;
END;

IF OBJECT_ID(N'tempdb..#mtp_os') IS NOT NULL
BEGIN
DROP TABLE #mtp_os;
END;

IF OBJECT_ID(N'tempdb..#mtp_latest') IS NOT NULL
BEGIN
DROP TABLE #mtp_latest;
END;

IF OBJECT_ID(N'tempdb..#mtp_return') IS NOT NULL
BEGIN
DROP TABLE #mtp_return;
END;

IF OBJECT_ID(N'tempdb..#dl') IS NOT NULL
BEGIN
DROP TABLE #dl;
END;

IF OBJECT_ID(N'tempdb..#bids') IS NOT NULL
BEGIN
DROP TABLE #bids;
END;

/*
Get a list of coal duids which have been out for at least 1 interval in the past 24 hours along with their technical information
*/
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
WITH dus AS
(
	SELECT
	duid
	FROM DISPATCH_UNIT_SCADA
	WHERE settlementdate > @24_hour_ago AND scadavalue < @mw_threshold AND duid IN ('{duid_list}')
	GROUP BY duid
	HAVING COUNT(*) > 0
)

SELECT TOP 1 WITH TIES
du.duid, du.registeredcapacity, du.maxrateofchangedown AS max_ramp_rate
INTO #duids
FROM DUDETAIL AS du
WHERE EXISTS
(
	SELECT 1 FROM dus WHERE dus.duid = du.duid
)
ORDER BY ROW_NUMBER() OVER(PARTITION BY du.duid ORDER BY du.effectivedate DESC, du.versionno DESC);
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------



/*
Solve the gaps and islands problem for the duids we found above
*/
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
WITH gi AS
(
	SELECT TOP 1 WITH TIES
	u.duid,
	DATETRUNC(DAY,DATEADD(HOUR, -4, u.outage_start)) AS tradingday,
	DATEADD(MINUTE, -5, u.outage_start) AS outage_start,
	DATEADD(MINUTE, -10, u.outage_start) AS os_minus_5,
	DATEADD(MINUTE, 5, u.outage_start) AS os_plus_5,
	CASE WHEN DATEDIFF(HOUR, u.outage_end, GETDATE()) >= 1 THEN u.outage_end END AS outage_end,
	CASE WHEN DATEDIFF(HOUR, u.outage_end, GETDATE()) < 1 THEN 'ONGOING' ELSE 'RETURNING' END AS current_status
	FROM
	(
		SELECT 
		s.duid, MIN(s.settlementdate) AS outage_start, MAX(s.settlementdate) AS outage_end
		FROM
		(
			SELECT 
			t.duid, t.settlementdate, t.rn1 - ROW_NUMBER() OVER (PARTITION BY t.duid ORDER BY t.settlementdate) AS gap
			FROM
			(
				SELECT dus.duid, dus.settlementdate, dus.scadavalue, ROW_NUMBER() OVER (PARTITION BY dus.duid ORDER BY dus.settlementdate) AS rn1
				FROM DISPATCH_UNIT_SCADA AS dus
				WHERE dus.settlementdate > @1000_day_ago AND EXISTS
				(
					SELECT 1 FROM #duids WHERE #duids.duid = dus.duid
				)
			) AS t
			WHERE t.scadavalue < @mw_threshold
		) AS s
		GROUP BY s.duid, s.gap
	) AS u
	ORDER BY ROW_NUMBER() OVER(PARTITION BY u.duid ORDER BY u.outage_start DESC)
)

SELECT 
gi.duid, gi.tradingday, gi.outage_start, gi.outage_end, gi.current_status, dus1.scadavalue AS mw_5min_prev, dus2.scadavalue AS actual_mw, #duids.max_ramp_rate, #duids.registeredcapacity
INTO #gi
FROM gi
INNER JOIN DISPATCH_UNIT_SCADA AS dus1 ON dus1.settlementdate = gi.outage_start AND dus1.duid = gi.duid
INNER JOIN DISPATCH_UNIT_SCADA AS dus2 ON dus2.settlementdate = gi.os_plus_5 AND dus2.duid = gi.duid
INNER JOIN #duids ON #duids.duid = gi.duid;


SELECT 
dot.duid, dot.bidofferdate, dot.bidsettlementdate
INTO #dot
FROM DISPATCHOFFERTRK AS dot
WHERE dot.bidtype = 'ENERGY' AND EXISTS
(
	SELECT 1 FROM #gi WHERE #gi.duid = dot.duid AND #gi.outage_start = dot.settlementdate
)
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------



/*
The main table with everything except MTPASA data
*/
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
SELECT
#gi.duid, #gi.tradingday, #gi.outage_start, #gi.outage_end, #gi.current_status, #gi.mw_5min_prev, #gi.actual_mw, #gi.max_ramp_rate, #gi.registeredcapacity, #dot.bidofferdate, #dot.bidsettlementdate
INTO #oid
FROM #gi
LEFT JOIN #dot ON #gi.duid = #dot.duid;
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------



/*
MTPASA data for the outage start
*/
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
WITH mtpi_os AS
(
	SELECT
	mtpda.duid, mtpda.day, MAX(mtpda.publish_datetime) AS max_publish_dt
	FROM MTPASA_DUIDAVAILABILITY AS mtpda
	WHERE mtpda.publish_datetime >= @1000_day_ago AND mtpda.day >= @1000_day_ago AND EXISTS
	(
		SELECT 1 FROM #oid WHERE #oid.tradingday = mtpda.day AND #oid.duid = mtpda.duid
	)
	GROUP BY mtpda.duid, mtpda.day
)

SELECT
mtpda.duid, mtpda.pasaavailability AS os_pasaavail, mtpda.pasaunitstate AS os_pasaunitstate
INTO #mtp_os
FROM MTPASA_DUIDAVAILABILITY AS mtpda
WHERE mtpda.publish_datetime >= @1000_day_ago AND mtpda.day >= @1000_day_ago AND EXISTS
(
	SELECT 1 FROM mtpi_os WHERE mtpi_os.day = mtpda.day AND mtpi_os.duid = mtpda.duid AND mtpi_os.max_publish_dt = mtpda.publish_datetime
);
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------



/*
Current MTPASA data
*/
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
WITH mtpi_current AS
(
	SELECT 
	mtpda.duid, mtpda.day, MAX(mtpda.publish_datetime) AS max_publish_dt
	FROM MTPASA_DUIDAVAILABILITY AS mtpda
	WHERE mtpda.publish_datetime >= @12_day_ago AND mtpda.day = @today AND EXISTS
	(
		SELECT 1 FROM #oid WHERE #oid.duid = mtpda.duid
	)
	GROUP BY mtpda.duid, mtpda.day
)

SELECT
mtpda.duid, mtpda.pasaavailability AS latest_pasaavail, mtpda.pasaunitstate AS latest_pasaunitstate
INTO #mtp_latest
FROM MTPASA_DUIDAVAILABILITY AS mtpda
WHERE mtpda.day = @today AND EXISTS
(
	SELECT 1 FROM mtpi_current WHERE mtpi_current.duid = mtpda.duid AND mtpi_current.day = mtpda.day AND mtpi_current.max_publish_dt = mtpda.publish_datetime
);
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------



/*
MTPASA data for expected return
*/
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
WITH mtpi_return1 AS
(
	SELECT 
	mtpda.duid, mtpda.day, MAX(mtpda.publish_datetime) AS max_publish_dt
	FROM MTPASA_DUIDAVAILABILITY AS mtpda
	WHERE mtpda.publish_datetime > @12_day_ago AND mtpda.day >= @today AND EXISTS
	(
		SELECT 1 FROM #oid WHERE #oid.duid = mtpda.duid AND mtpda.day >= #oid.tradingday AND #oid.current_status = 'ONGOING'
	)
	GROUP BY mtpda.duid, mtpda.day
),


mtpi_return2 AS
(
	SELECT 
	mtpda.duid, mtpda.publish_datetime, MIN(mtpda.day) AS expected_return
	FROM MTPASA_DUIDAVAILABILITY AS mtpda
	WHERE mtpda.pasaavailability > 0 AND EXISTS
	(
		SELECT 1 FROM mtpi_return1 WHERE mtpi_return1.duid = mtpda.duid AND mtpi_return1.max_publish_dt = mtpda.publish_datetime AND mtpi_return1.day = mtpda.day 
	)
	GROUP BY mtpda.duid, mtpda.publish_datetime
)


SELECT TOP 1 WITH TIES
mtpda.duid, mtpda.day AS expected_return, mtpda.latest_offer_datetime
INTO #mtp_return
FROM MTPASA_DUIDAVAILABILITY AS mtpda
WHERE EXISTS
(
	SELECT 1 FROM mtpi_return2 WHERE mtpi_return2.duid = mtpda.duid AND mtpi_return2.publish_datetime = mtpda.publish_datetime AND mtpi_return2.expected_return = mtpda.day
)
ORDER BY ROW_NUMBER() OVER(PARTITION BY mtpda.duid ORDER BY mtpda.publish_datetime DESC)

---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------

/*
Dispatch target data
*/
---------------------------------------------------------------------------------------------------
---------------------------------------------------------------------------------------------------
SELECT 
dl.settlementdate, dl.duid, dl.totalcleared AS target_mw
INTO #dl
FROM DISPATCHLOAD AS dl
WHERE EXISTS
(
	SELECT 1 FROM #oid WHERE #oid.duid = dl.duid AND #oid.outage_start = dl.settlementdate
)


SELECT
bdo.duid, bdo.bidtype, bdo.rebidexplanation, bdo.rebid_category, bdo.entrytype
INTO #bids
FROM BIDDAYOFFER AS bdo
WHERE bdo.bidtype = 'ENERGY' AND EXISTS
(
	SELECT 1 FROM #oid WHERE #oid.duid = bdo.duid AND #oid.bidofferdate = bdo.offerdate AND #oid.bidsettlementdate = bdo.settlementdate
)

SELECT
#oid.duid, 
#oid.outage_start, 
#oid.outage_end, 
#oid.current_status, 
#oid.mw_5min_prev, 
#oid.actual_mw,
#dl.target_mw,
#oid.max_ramp_rate, 
#oid.registeredcapacity,
#oid.bidofferdate, 
#oid.bidsettlementdate, 
#bids.rebidexplanation,
#bids.rebid_category,
#mtp_os.os_pasaavail, 
#mtp_os.os_pasaunitstate,
#mtp_latest.latest_pasaavail,
#mtp_latest.latest_pasaunitstate,
#mtp_return.expected_return,
#mtp_return.latest_offer_datetime
FROM #oid
INNER JOIN #mtp_os ON #mtp_os.duid = #oid.duid
INNER JOIN #mtp_latest ON #mtp_latest.duid = #oid.duid
LEFT JOIN #mtp_return ON #mtp_return.duid = #oid.duid
LEFT JOIN #dl ON #oid.duid = #dl.duid
LEFT JOIN #bids ON #bids.duid = #oid.duid
/***************************************************************************************************
*	Coal generation outage query - identifies offline periods and consolidates bid availability,
*	rebid information and MTPASA data to assist in outage classification for NEM coal-fired units.
*---------------------------------------------------------------------------------------------------
*	Variable declaration section. Parameters controlling date range and "offline threshold".
*	Set @STARTDATE and @ENDDATE values to first and last calendar dates of range to be scanned.
*	This version of the query returns MTPASA data from the MTPASA_DUIDAVAILABILITY table but will not
*	return data for dates earlier than 23-Aug-2020, the earliest date for which DUID-level MTPASA data
*	became public. For earlier date ranges use the _preMTPASA version of the query.
*
*	@THRESHOLD variable sets the MW output level below which a unit is treated as offline.
*	Too high a threshold can result in 'false positives' - ie reported short outages -  eg as unit  
*	output varies at low levels when first resynchronising after an outage. 
*	The suggested value is 4 MW. To capture short trips of Stanwell PS units, which have a
*	'trip to house load' (TTHL) functionality allowing the units to fall back to ~15MW while effectively
*	disconnected from the grid, a second threshold parameter @TTHL_STAN with a default value of 16 MW
*	is used for those units only - this can be set to the same value as @THRESHOLD if not wanting to
*	capture such trips, since 16 MW will also report some 'false positives' for Stanwell.
****************************************************************************************************/
DECLARE @STARTDATE DATETIME, @ENDDATE DATETIME;
SET @STARTDATE = '{start_date}'
SET @ENDDATE = '{end_date}'
DECLARE @THRESHOLD FLOAT = 4.0
DECLARE @TTHL_STAN FLOAT = 16.0

DECLARE @FIRSTDI DATETIME, @LASTDI DATETIME, @FIRSTBIDDAY date, @LASTBIDDAY date
SET @FIRSTDI = DATEADD(mi, 5, @STARTDATE)
SET @LASTDI = DATEADD(d, 1, @ENDDATE)
SET @FIRSTBIDDAY = cast(DATEADD(mi, -240, @FIRSTDI) as date)
SET @LASTBIDDAY = cast(DATEADD(mi, -240, @LASTDI) as date)
;

With
CoalUnits as (
/***************************************************************************************************
*	Controls which units are checked - currently only operating and recently retired coal-fired units.
*	If history for older retired units is required, add them to the list below.
****************************************************************************************************/
	select distinct DUID, REGIONID, STATIONID
	from DUDETAILSUMMARY
	where DUID in ('BW01', 'BW02', 'BW03', 'BW04', 'CALL_B1', 'CALL_B2', 'CPP_3', 'CPP_4', 'ER01', 'ER02', 'ER03', 'ER04'
					, 'GSTONE1', 'GSTONE2', 'GSTONE3', 'GSTONE4', 'GSTONE5', 'GSTONE6', 'KPP_1'
					, 'LOYYB1', 'LOYYB2', 'LYA1', 'LYA2', 'LYA3', 'LYA4'
					, 'MP1', 'MP2', 'MPP_1', 'MPP_2', 'STAN-1', 'STAN-2', 'STAN-3', 'STAN-4'
					, 'TARONG#1', 'TARONG#2', 'TARONG#3', 'TARONG#4', 'TNPS1', 'VP5', 'VP6'
					, 'YWPS1', 'YWPS2', 'YWPS3', 'YWPS4')
	),
LatestBids as (
/***************************************************************************************************
*	Gets applicable bid data - Availability, PASA Availability, and Rebid reasons:
*	- for the current dispatch interval (DI), and
*	- for dispatch intervals 30 minutes before, 30 minutes after and 60 minutes after current DI.
*	These can assist in analysis of why a unit has gone offline.
****************************************************************************************************/
	select DATEFROMPARTS(YEAR(DATEADD(mi, -1, interval_datetime)), MONTH(DATEADD(mi, -1, interval_datetime)), DAY(DATEADD(mi, -1, interval_datetime))) SETTLEMENTDAY
	, interval_datetime settlementdate_bid, BIDPEROFFER_D.DUID, MAXAVAIL, PASAAVAILABILITY, BIDPEROFFER_D.OFFERDATE
	, ISNULL(REBIDEXPLANATION, ENTRYTYPE) REASON, REBID_CATEGORY, REBID_EVENT_TIME, REBID_AWARE_TIME, REBID_DECISION_TIME
	, LAG(MAXAVAIL, 6) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) MAXAVAIL_HHBACK
	, LAG(PASAAVAILABILITY, 6) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) PASAAVAIL_HHBACK
	, LAG(ISNULL(REBIDEXPLANATION, ENTRYTYPE), 6) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) REASON_HHBACK
	, LEAD(MAXAVAIL, 6) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) MAXAVAIL_HHFWD
	, LEAD(PASAAVAILABILITY, 6) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) PASAAVAIL_HHFWD
	, LEAD(ISNULL(REBIDEXPLANATION, ENTRYTYPE), 6) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) REASON_HHFWD
	, LEAD(BIDPEROFFER_D.OFFERDATE, 6) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) OFFERTIME_HHFWD
	, LEAD(MAXAVAIL, 12) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) MAXAVAIL_1HFWD
	, LEAD(PASAAVAILABILITY, 12) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) PASAAVAIL_1HFWD
	, LEAD(ISNULL(REBIDEXPLANATION, ENTRYTYPE), 12) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) REASON_1HFWD
	, LEAD(BIDPEROFFER_D.OFFERDATE, 12) over (PARTITION BY BIDPEROFFER_D.DUID ORDER BY INTERVAL_DATETIME) OFFERTIME_1HFWD
	from BIDPEROFFER_D
	INNER JOIN CoalUnits
	on BIDPEROFFER_D.DUID = CoalUnits.DUID and BIDTYPE = 'ENERGY'
	INNER JOIN BIDDAYOFFER
	on BIDDAYOFFER.DUID = BIDPEROFFER_D.DUID and BIDDAYOFFER.SETTLEMENTDATE = BIDPEROFFER_D.SETTLEMENTDATE
		and BIDDAYOFFER.OFFERDATE = BIDPEROFFER_D.OFFERDATE and BIDDAYOFFER.BIDTYPE = 'ENERGY'
	where INTERVAL_DATETIME between DATEADD(mi, -30, @FIRSTDI) and DATEADD(mi, 60, @LASTDI)
	),
MTPASA as (
/***************************************************************************************************
*	Gets most recently published MTPASA daily availability at DUID level, as well as date of last update 
*	(which may be up to 7 days before the relevant calendar day, due to weekly rollover of MTPASA schedules).
*	This data may identify an outage flagged in advance in MTPASA (making it somewhat less likely
*	to have been Forced - but see the note below), as well as cases where a unit is taken offline for
*	commercial (rather than technical) reasons, in which case it will remain bid available in MTPASA.
*
*	##	NOTE that outages flagged ahead in MTPASA are not always 'Planned' and can instead be Forced
*		outages which allow some delay in the unit coming offline for commercial or system reliability
*		reasons. Additional contextual information may be required to distinguish these cases.
****************************************************************************************************/
	
    select DAY, mtp.DUID, PASAAVAILABILITY MTPASA_AVAIL, PASAUNITSTATE, 1 + DATEDIFF(hour, PUBLISH_DATETIME, DAY) / 24 as MTPASA_AGE, LATEST_OFFER_DATETIME MTPASA_OFFERDATE
    from MTPASA_DUIDAVAILABILITY mtp
    INNER JOIN CoalUnits
    ON CoalUnits.DUID = mtp.DUID
    where DAY between @STARTDATE and @ENDDATE
    and PUBLISH_DATETIME = (select max(a.PUBLISH_DATETIME) from MTPASA_DUIDAVAILABILITY a where a.DAY = mtp.DAY)
	),
FlagOutages as (
/***************************************************************************************************
*	Base query on DISPATCHLOAD table to pick up changes in unit state between 'online' and 'offline',
*	based on INITIALMW vs the @THRESHOLD parameter. A second leg of the test filters for spurious 
*	INITIALMW values where nearby AVAILABILITY and TOTALCLEARED (target output) values are zero.
****************************************************************************************************/
	select settlementdate, dl.duid, STATIONID, REGIONID, initialmw
	, lag(TOTALCLEARED, 1, INITIALMW) over (PARTITION BY dl.DUID ORDER BY settlementdate) targetmw
	, case when initialmw < (case when STATIONID = 'STANWELL' then @TTHL_STAN else @THRESHOLD end)
				then 1 
			when (sum(AVAILABILITY+TOTALCLEARED)
						over (PARTITION BY dl.DUID ORDER BY settlementdate ROWS 2 PRECEDING)) = 0
					and INITIALMW < 20
				then 1
			else 0 end outageflag
	from dispatchload dl
	INNER JOIN CoalUnits
	on CoalUnits.DUID = dl.DUID
	where settlementdate between @FIRSTDI and @LASTDI
		and INTERVENTION = (select max(cast(INTERVENTION as INT)) from DISPATCHCASESOLUTION where SETTLEMENTDATE = dl.SETTLEMENTDATE)
	),
OutageLength as (
/***************************************************************************************************
*	Uses offline flags from previous subquery to set up the Offline count variable used to determine 
*	outage length in DIs. Also determines the first and last DIs in which data exists for each DUID
*	in the overall date range scanned (normally the full period unless a unit is retired). 
*	The outage length calculations in the main query block assume continuous DI data for each DUID 
*	within the analysis period, which is always true for the DISPATCHLOAD source.
****************************************************************************************************/
	select settlementdate, duid, STATIONID, REGIONID, initialmw, targetmw, outageflag
	, lag(outageflag, 1, 0) over (partition by duid order by settlementdate) lag_outage
	, sum(outageflag) over (partition by duid order by settlementdate ROWS BETWEEN CURRENT ROW AND UNBOUNDED FOLLOWING) countout
	, min(settlementdate) over (partition by duid) MinSD, max(settlementdate) over (partition by duid) MaxSD
	from FlagOutages
	)

/***************************************************************************************************
*	Main query block bringing together data from the outage identification queries with:
*	-	bid data for the first 'offline' DI as well as 'nearby' DIs (-30 minutes, +30 and +60 minutes)
*	-	MTPASA availability for the calendar day in which the first offline DI falls.
*	Also flags whether outage represents Continuation of an outage current at the start of the analysis period
*	(ie the unit was offline in the first DI of the period), and/or was Unfinished at the end of the period
*	(ie the unit was on outage and remained offline in the last DI for which that DUID has output data).
****************************************************************************************************/
select OL.DUID, STATIONID, REGIONID
	, SETTLEMENTDATE OutageFrom
	, DATEADD(mi, 5*(countout - (lead(countout, 1, 0) OVER (PARTITION BY OL.DUID ORDER BY SETTLEMENTDATE)) - 1), SETTLEMENTDATE) OutageTo
	, YEAR(DATEADD(mi, -1, SETTLEMENTDATE)) StartYear
	, YEAR(DATEADD(mi, -1 + 5*(countout - (lead(countout, 1, 0) OVER (PARTITION BY OL.DUID ORDER BY SETTLEMENTDATE)) - 1), SETTLEMENTDATE)) EndYear
	, countout - (lead(countout, 1, 0) OVER (PARTITION BY OL.DUID ORDER BY SETTLEMENTDATE)) OutageLength_DIs
	, (countout - (lead(countout, 1, 0) OVER (PARTITION BY OL.DUID ORDER BY SETTLEMENTDATE))) / 288.0 OutageLength_days
	, INITIALMW, targetmw
	, MAXAVAIL, PASAAVAILABILITY, OFFERDATE, REASON, REBID_CATEGORY, REBID_EVENT_TIME, REBID_AWARE_TIME, REBID_DECISION_TIME
	, MAXAVAIL_HHBACK, PASAAVAIL_HHBACK, REASON_HHBACK, MAXAVAIL_HHFWD, PASAAVAIL_HHFWD, REASON_HHFWD, OFFERTIME_HHFWD
	, MAXAVAIL_1HFWD, PASAAVAIL_1HFWD, REASON_1HFWD, OFFERTIME_1HFWD
	, MTPASA_AVAIL, PASAUNITSTATE, MTPASA_AGE, MTPASA_OFFERDATE
	, MinSD, MaxSD, 1 + (DATEDIFF(mi, MinSD, MaxSD) / 5) TotalDIs
	, case when SETTLEMENTDATE = MinSD then 1 else 0 end Continuation
	, case when
		DATEADD(mi, 5*(countout - (lead(countout, 1, 0) OVER (PARTITION BY OL.DUID ORDER BY SETTLEMENTDATE)) - 1), SETTLEMENTDATE) = MaxSD
		then 1 else 0 end Unfinished
from (select * from OutageLength where outageflag - lag_outage = 1) OL 
INNER JOIN LatestBids
on SETTLEMENTDATE = LatestBids.settlementdate_bid and OL.DUID = LatestBids.DUID
INNER JOIN MTPASA
on MTPASA.DUID = LatestBids.DUID and MTPASA.DAY = SETTLEMENTDAY
order by DUID, SETTLEMENTDATE
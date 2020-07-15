-- create table with target field names
CREATE TEMP TABLE dof_pts_propmaster as (
	SELECT 
		v,
		parid as bbl,
		boro,
		block as tb,
		lot as tl,
		street_name,
		housenum_lo,
		housenum_hi,
		aptno,
		zip_code as zip,
		bldg_class as BLDGCL,
		ease,
		av_owner as owner,
		REPLACE(land_area, '+', '')::double precision as LAND_AREA,
		REPLACE(gross_sqft, '+', '')::double precision as GROSS_SQFT,
		REPLACE(residential_area_gross, '+', '')::double precision as RESIDAREA,
		REPLACE(office_area_gross, '+', '')::double precision as OFFICEAREA,
		REPLACE(retail_area_gross, '+', '')::double precision as RETAILAREA,
		REPLACE(garage_area, '+', '')::double precision as GARAGEAREA,
		REPLACE(storage_area_gross, '+', '')::double precision as STORAGEAREA,
		REPLACE(factory_area_gross, '+', '')::double precision as FACTORYAREA,
		REPLACE(other_area_gross, '+', '')::double precision as OTHERAREA,
		REPLACE(num_bldgs, '+', '')::double precision as BLDGS,
		REPLACE(bld_story, '+', '')::double precision as STORY,
		REPLACE(coop_apts, '+', '')::double precision as COOP_APTS,
		REPLACE(units, '+', '')::double precision as UNITS,

		round(REPLACE(lot_frt, '+', '')::numeric, 2) as LFFT,

		(CASE 
			WHEN lot_dep <> 'ACRE' 
			THEN round(REPLACE(lot_dep, '+', '')::numeric, 2)
			ELSE NULL 
		END) as LDFT,

		round(REPLACE(bld_frt, '+', '')::numeric, 2) as BFFT,
		round(REPLACE(bld_dep, '+', '')::numeric, 2) as BDFT,

		bld_ext as ext,
		lot_irreg as IRREG,
		REPLACE(curactland, '+', '')::double precision as CURAVL_ACT,
		-- pyactland
		REPLACE(curacttot, '+', '')::double precision as CURAVT_ACT,
		-- pyacttot
		REPLACE(curactextot, '+', '')::double precision as CUREXT_ACT,
		-- pyactextot
		yrbuilt,
		yralt1,
		yralt2,
		condo_number,
		appt_boro as AP_BORO,
		appt_block as AP_BLOCK,
		appt_lot as AP_LOT,
		appt_ease as AP_EASE,
		appt_date as AP_DATE
	FROM pluto_pts.latest
);

CREATE TEMP TABLE pluto_rpad_geo as (
	WITH 
	pluto_rpad_rownum AS (
		SELECT 
			a.*, 
			ROW_NUMBER() OVER (
				PARTITION BY boro||tb||tl
				ORDER BY 
					curavt_act::numeric DESC, 
					land_area::numeric DESC, 
					ease ASC
			) AS row_number
		FROM dof_pts_propmaster a
	),
	pluto_rpad_sub AS (
		SELECT * 
		FROM pluto_rpad_rownum 
		WHERE row_number = 1
	)
	SELECT 
		a.*, 
		b.billingbbl,
		b.bbl as geo_bbl,
		b.communitydistrict as cd,
		b.censustract2010 as ct2010,
		b.censusblock2010 as cb2010,
		b.communityschooldistrict as schooldist,
		b.citycouncildistrict as council,
		b.zipcode,
		b.firecompanynumber as firecomp,
		b.policeprecinct as policeprct,
		b.healthcenterdistrict,
		b.healtharea,
		b.sanitationdistrict as sanitdistrict,
		b.sanitationcollectionscheduling as sanitsub,
		b.boepreferredstreetname,
		b.taxmapnumbersectionandvolume as taxmap,
		b.sanbornmapidentifier,
		b.latitude,
		b.longitude,
		coalesce(
			b.xcoord::integer, 
			ST_X(ST_TRANSFORM(b.wkb_geometry, 2263))::integer
		) as xcoord,
		coalesce(
			b.ycoord::integer, 
			ST_Y(ST_TRANSFORM(b.wkb_geometry, 2263))::integer
		) as ycoord,
		b.grc,
		b.grc2,
		b.msg,
		b.msg2,
		b.borough,
		b.block,
		b.lot,
		b.easement,
		b.input_hnum,
		b.input_sname,
		b.numberofexistingstructures as numberOfExistingStructuresOnLot,
		b.wkb_geometry AS geom
	FROM pluto_rpad_sub a
	LEFT JOIN pluto_input_geocodes.latest b
	ON a.boro||a.tb||a.tl=b.borough||lpad(b.block,5,'0')||lpad(b.lot,4,'0')
)

\COPY pluto_rpad_geo TO PSTDOUT DELIMITER ',' CSV HEADER;
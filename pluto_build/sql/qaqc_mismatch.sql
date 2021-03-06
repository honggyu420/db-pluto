DELETE FROM dcp_pluto.qaqc_mismatch 
WHERE pair = :'VERSION'||' - '||:'VERSION_PREV' 
AND CONDO::boolean = :CONDO
AND MAPPED::boolean = :MAPPED;

INSERT INTO dcp_pluto.qaqc_mismatch (
SELECT
    :'VERSION'||' - '||:'VERSION_PREV' as pair, 
	:CONDO as condo,
    :MAPPED as mapped,
    count(*) as total,
    count(nullif(a.borough = b.borough, true)) as borough,
    count(nullif(a.block::numeric = b.block::numeric, true)) as block,
    count(nullif(a.lot::numeric = b.lot::numeric, true)) as lot,
    count(nullif(a.cd::numeric = b.cd::numeric, true)) as cd,
    count(nullif(a.ct2010 = b.ct2010, true)) as ct2010,
    count(nullif(a.cb2010 = b.cb2010, true)) as cb2010,
    count(nullif(a.schooldist = b.schooldist, true)) as schooldist,
    count(nullif(a.council::numeric = b.council::numeric, true)) as council,
    count(nullif(a.zipcode::numeric = b.zipcode::numeric, true)) as zipcode,
    count(nullif(a.firecomp = b.firecomp, true)) as firecomp,
    count(nullif(a.policeprct::numeric = b.policeprct::numeric, true)) as policeprct,
    count(nullif(a.healtharea::numeric = b.healtharea::numeric, true)) as healtharea,
    count(nullif(a.sanitboro = b.sanitboro, true)) as sanitboro,
    count(nullif(a.sanitsub = b.sanitsub, true)) as sanitsub,
    count(nullif(trim(a.address) = trim(b.address), true)) as address,
    count(nullif(a.zonedist1 = b.zonedist1, true)) as zonedist1,
    count(nullif(a.zonedist2 = b.zonedist2, true)) as zonedist2,
    count(nullif(a.zonedist3 = b.zonedist3, true)) as zonedist3,
    count(nullif(a.zonedist4 = b.zonedist4, true)) as zonedist4,
    count(nullif(a.overlay1 = b.overlay1, true)) as overlay1,
    count(nullif(a.overlay2 = b.overlay2, true)) as overlay2,
    count(nullif(a.spdist1 = b.spdist1, true)) as spdist1,
    count(nullif(a.spdist2 = b.spdist2, true)) as spdist2,
    count(nullif(a.spdist3 = b.spdist3, true)) as spdist3,
    count(nullif(a.ltdheight = b.ltdheight, true)) as ltdheight,
    count(nullif(a.splitzone = b.splitzone, true)) as splitzone,
    count(nullif(a.bldgclass = b.bldgclass, true)) as bldgclass,
    count(nullif(a.landuse = b.landuse, true)) as landuse,
    count(nullif(a.easements::numeric = b.easements::numeric, true)) as easements,
    count(nullif(a.ownertype = b.ownertype, true)) as ownertype,
    count(nullif(a.ownername = b.ownername, true)) as ownername,
    count(nullif(abs(a.lotarea::numeric-b.lotarea::numeric) <5, true)) as lotarea,
    count(nullif(abs(a.bldgarea::numeric-b.bldgarea::numeric) <5, true)) as bldgarea,
    count(nullif(abs(a.comarea::numeric-b.comarea::numeric) <5, true)) as comarea,
    count(nullif(abs(a.resarea::numeric-b.resarea::numeric) <5, true)) as resarea,
    count(nullif(abs(a.officearea::numeric-b.officearea::numeric) <5, true)) as officearea,
    count(nullif(abs(a.retailarea::numeric-b.retailarea::numeric) <5, true)) as retailarea,
    count(nullif(abs(a.garagearea::numeric-b.garagearea::numeric) <5, true)) as garagearea,
    count(nullif(abs(a.strgearea::numeric-b.strgearea::numeric) <5, true)) as strgearea,
    count(nullif(abs(a.factryarea::numeric-b.factryarea::numeric) <5, true)) as factryarea,
    count(nullif(abs(a.otherarea::numeric-b.otherarea::numeric) <5, true)) as otherarea,
    count(nullif(a.areasource = b.areasource, true)) as areasource,
    count(nullif(a.numbldgs::numeric = b.numbldgs::numeric, true)) as numbldgs,
    count(nullif(a.numfloors::numeric = b.numfloors::numeric, true)) as numfloors,
    count(nullif(a.unitsres::numeric = b.unitsres::numeric, true)) as unitsres,
    count(nullif(a.unitstotal::numeric = b.unitstotal::numeric, true)) as unitstotal,
    count(nullif(abs(a.lotfront::numeric-b.lotfront::numeric) <5, true)) as lotfront,
    count(nullif(abs(a.lotdepth::numeric-b.lotdepth::numeric) <5, true)) as lotdepth,
    count(nullif(abs(a.bldgfront::numeric-b.bldgfront::numeric) <5, true)) as bldgfront,
    count(nullif(abs(a.bldgdepth::numeric-b.bldgdepth::numeric) <5, true)) as bldgdepth,
    count(nullif(a.ext = b.ext, true)) as ext,
    count(nullif(a.proxcode = b.proxcode, true)) as proxcode,
    count(nullif(a.irrlotcode = b.irrlotcode, true)) as irrlotcode,
    count(nullif(a.lottype = b.lottype, true)) as lottype,
    count(nullif(a.bsmtcode = b.bsmtcode, true)) as bsmtcode,
    count(nullif(abs(a.assessland::numeric-b.assessland::numeric) <10, true)) as assessland,
    count(nullif(abs(a.assesstot::numeric-b.assesstot::numeric) <10, true)) as assesstot,
    count(nullif(abs(a.exempttot::numeric-b.exempttot::numeric) <10, true)) as exempttot,
    count(nullif(a.yearbuilt::numeric = b.yearbuilt::numeric, true)) as yearbuilt,
    count(nullif(a.yearalter1::numeric = b.yearalter1::numeric, true)) as yearalter1,
    count(nullif(a.yearalter2::numeric = b.yearalter2::numeric, true)) as yearalter2,
    count(nullif(a.histdist = b.histdist, true)) as histdist,
    count(nullif(a.landmark = b.landmark, true)) as landmark,
    count(nullif(a.builtfar::double precision = b.builtfar::double precision, true)) as builtfar,
    count(nullif(a.residfar::double precision = b.residfar::double precision, true)) as residfar,
    count(nullif(a.commfar::double precision = b.commfar::double precision, true)) as commfar,
    count(nullif(a.facilfar::double precision = b.facilfar::double precision, true)) as facilfar,
    count(nullif(a.borocode::numeric = b.borocode::numeric, true)) as borocode,
    count(nullif(a.bbl::double precision = b.bbl::double precision, true)) as bbl,
    count(nullif(a.condono::numeric = b.condono::numeric, true)) as condono,
    count(nullif(a.tract2010 = b.tract2010, true)) as tract2010,
    count(nullif(abs(a.xcoord::numeric-b.xcoord::numeric) <1, true)) as xcoord,
    count(nullif(abs(a.ycoord::numeric-b.ycoord::numeric) <1, true)) as ycoord,
    count(nullif(abs(a.latitude::numeric-b.latitude::numeric) <0.0001, true)) as latitude,
    count(nullif(abs(a.longitude::numeric-b.longitude::numeric) <0.0001, true)) as longitude,
    count(nullif(a.zonemap = b.zonemap, true)) as zonemap,
    count(nullif(a.zmcode = b.zmcode, true)) as zmcode,
    count(nullif(a.sanborn = b.sanborn, true)) as sanborn,
    count(nullif(a.taxmap = b.taxmap, true)) as taxmap,
    count(nullif(a.edesignum = b.edesignum, true)) as edesignum,
    count(nullif(a.appbbl::double precision = b.appbbl::double precision, true)) as appbbl,
    count(nullif(a.appdate = b.appdate, true)) as appdate,
    count(nullif(a.plutomapid = b.plutomapid, true)) as plutomapid,
    count(nullif(a.sanitdistrict = b.sanitdistrict, true)) as sanitdistrict,
    count(nullif(a.healthcenterdistrict::numeric = b.healthcenterdistrict::numeric, true)) as healthcenterdistrict,
    count(nullif(a.firm07_flag = b.firm07_flag, true)) as firm07_flag,
    count(nullif(a.pfirm15_flag = b.pfirm15_flag, true)) as pfirm15_flag
    FROM dcp_pluto.:"VERSION" a
INNER JOIN dcp_pluto.:"VERSION_PREV" b
ON (a.bbl::bigint = b.bbl::bigint)
:CONDITION)
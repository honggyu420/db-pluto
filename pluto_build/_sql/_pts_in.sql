DROP TABLE IF EXISTS pluto_rpad_geo;
CREATE TABLE pluto_rpad_geo (
    v text,
    bbl text,
    boro text,
    tb text,
    tl text,
    street_name text,
    housenum_lo text,
    housenum_hi text,
    aptno text,
    zip text,
    bldgcl text,
    ease text,
    owner text,
    land_area double precision,
    gross_sqft double precision,
    residarea double precision,
    officearea double precision,
    retailarea double precision,
    garagearea double precision,
    storagearea double precision,
    factoryarea double precision,
    otherarea double precision,
    bldgs double precision,
    story double precision,
    coop_apts double precision,
    units double precision,
    lfft numeric,
    ldft numeric,
    bfft numeric,
    bdft numeric,
    ext text,
    irreg text,
    curavl_act double precision,
    curavt_act double precision,
    curext_act double precision,
    yrbuilt text,
    yralt1 text,
    yralt2 text,
    condo_number text,
    ap_boro text,
    ap_block text,
    ap_lot text,
    ap_ease text,
    ap_date text,
    row_number bigint,
    billingbbl text,
    geo_bbl text,
    cd text,
    ct2010 text,
    cb2010 text,
    schooldist text,
    council text,
    zipcode text,
    firecomp text,
    policeprct text,
    healthcenterdistrict text,
    healtharea text,
    sanitdistrict text,
    sanitsub text,
    boepreferredstreetname text,
    taxmap text,
    sanbornmapidentifier text,
    latitude text,
    longitude text,
    xcoord integer,
    ycoord integer,
    grc text,
    grc2 text,
    msg text,
    msg2 text,
    borough text,
    block text,
    lot text,
    easement text,
    input_hnum text,
    input_sname text,
    numberofexistingstructuresonlot text,
    geom geometry(Geometry,4326)
);

\COPY pluto_rpad_geo FROM PSTDIN DELIMITER ',' CSV HEADER;
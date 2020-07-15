#!/bin/bash
source bin/config.sh

# echo "Starting to build PLUTO ..."
# psql $BUILD_ENGINE -f _sql/preprocessing.sql
psql -q $RECIPE_ENGINE -f _sql/_pts_out.sql | 
    psql $BUILD_ENGINE -f _sql/_pts_in.sql
# psql $BUILD_ENGINE -f sql/create_rpad_geo.sql

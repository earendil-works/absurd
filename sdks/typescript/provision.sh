#!/bin/sh
dropdb absurd
createdb absurd
#psql -d absurd -f ../../sql/absurd.sql
psql -d absurd -f ../../sql/absurd_clean.sql
#psql -d absurd -c "select absurd.create('provisioning-demo');"

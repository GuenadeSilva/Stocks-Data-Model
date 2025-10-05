#!/bin/bash
dbt compile --target prod
mkdir -p target_prod
mv target/manifest.json ./target_prod/manifest.json

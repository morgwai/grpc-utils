#!/bin/bash
# Copyright 2021 Piotr Morgwai Kotarbinski, Licensed under the Apache License, Version 2.0
mvnLocalRepo=$(./mvnw -q -N exec:exec -Dexec.executable=echo '-Dexec.args=${settings.localRepository}') &&
./mvnw dependency:resolve -Dclassifier=javadoc -DincludeScope=compile -DexcludeTransitive=true \
	-DexcludeGroupIds=io.grpc &&

echo "extracting package/element lists..." &&
cd "${mvnLocalRepo}" &&
find . -name '*-javadoc.jar' |rev |cut -d / -f '2-' |rev | while read folder ; do
	cd "${folder}" &&
	unzip -n -q *javadoc.jar '*-list' ;
	cd "${mvnLocalRepo}" ;
done &&
mkdir -p "${mvnLocalRepo}/io/grpc/grpc-all/" &&
echo "io.grpc" >"${mvnLocalRepo}/io/grpc/grpc-all/package-list" &&
echo "setup successful :)"

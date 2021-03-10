#!/usr/bin/env bash

set -e

# Original
# sbt -Dversion='0.6.1-SNAPSHOT' -Dscala.version='2.12.10' -Dspark.version='2.4.3' assemblyNoTest

# New configurations
# sbt -Dversion='0.6.1-SNAPSHOT' -Dscala.version='2.12.13' -Dspark.version='2.4.3' assemblyNoTest
# sbt -Dversion='0.6.1-SNAPSHOT' -Dscala.version='2.12.10' -Dspark.version='3.0.2' assemblyNoTest
sbt -Dversion='0.6.1-SNAPSHOT' -Dscala.version='2.12.13' -Dspark.version='3.0.2' assemblyNoTest

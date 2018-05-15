<?php

$ini_array = parse_ini_file(
  "spanner.grpc.config",
  false);
print_r($ini_array);

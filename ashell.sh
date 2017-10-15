#!/bin/bash

docker exec -it compose_dn0.dev_1 sh -c "echo password | kinit $1;accumulo shell"

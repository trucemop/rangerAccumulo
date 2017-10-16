#!/bin/bash

if [ "$1" == "accumulo" ]; then
	docker exec -it compose_dn0.dev_1 sh -c "kinit -kt /etc/security/keytabs/accumulo.headless.keytab accumulo-dev@EXAMPLE.COM;accumulo shell"

else

	docker exec -it compose_dn0.dev_1 sh -c "echo password | kinit $1;accumulo shell"

fi

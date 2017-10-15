#!/bin/bash

kinit -kt /etc/security/keytabs/accumulo.headless.keytab accumulo-dev@EXAMPLE.COM

accumulo shell -f /root/statements

#!/bin/bash

set -e 
set -x

export GOBIN="/home/pandora/nimbus.io/bin"
export NIMBUSIO_WILDCARD_SSL_CERT="/home/pandora/keys/cacert.pem"
export NIMBUSIO_WILDCARD_SSL_KEY="/home/pandora/keys/privkey.pem"
export NIMBUSIO_WEB_DIRECTOR_ADDR="dev.nimbus.io"
export NIMBUSIO_WEB_DIRECTOR_PORT="9443"
export NIMBUS_IO_SERVICE_DOMAIN="dev.nimbus.io"

exec $GOBIN/webdirector &> /var/log/pandora/nimbus/webdirector.log


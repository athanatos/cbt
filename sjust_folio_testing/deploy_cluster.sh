#!/bin/bash
set -x

CEPH_SHA1=9d465d1f4bbf3adc8c5d0a2ed062f92b7dccc555

# download cephadm

CEPHADM_RELEASE=18.2.1
BINDIR=/home/sjust/bin
cd ${BINDIR}
curl --silent --remote-name --location https://download.ceph.com/rpm-${CEPHADM_RELEASE}/el9/noarch/cephadm
chmod +x cephadm

# bootstrap cluster

./cephadm \
  --image quay.ceph.io/ceph-ci/ceph:${CEPH_SHA1}-crimson \
	bootstrap \
	--allow-mismatched-release \
	--log-to-file \
	--mon-ip 127.0.0.1

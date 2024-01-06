#!/bin/bash
set -x

CEPH_SHA1=9d465d1f4bbf3adc8c5d0a2ed062f92b7dccc555

# download cephadm

CEPHADM_RELEASE=18.2.1
BINDIR=/home/sjust/bin
cd ${BINDIR}
curl --silent --remote-name --location https://download.ceph.com/rpm-${CEPHADM_RELEASE}/el9/noarch/cephadm
chmod +x cephadm

# clear old cluster

if sudo ./cephadm shell -- ceph -s ; then
  sudo ./cephadm shell -- ceph mgr module disable cephadm
	FSID = $(sudo ./cephadm shell -- ceph fsid)
	sudo ./cephadm rm-cluster --force --zap-osds --fsid ${FSID}
fi

# bootstrap cluster

sudo ./cephadm \
  --image quay.ceph.io/ceph-ci/ceph:${CEPH_SHA1}-crimson \
	bootstrap \
	--allow-mismatched-release \
	--log-to-file \
	--mon-ip 127.0.0.1 \
	--skip-mon-network

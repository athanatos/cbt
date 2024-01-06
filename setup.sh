#!/bin/bash

# script to install CBT dependencies and tools for active benchmarking

sudo yum check-update
sudo yum -y update
sudo yum install -y psmisc util-linux coreutils xfsprogs e2fsprogs findutils \
  git wget bzip2 make automake gcc gcc-c++ kernel-devel perf blktrace lsof \
  sysstat screen python3-yaml ipmitool dstat zlib-devel pdsh iftop iperf3

git clone https://github.com/axboe/fio.git
git clone https://github.com/andikleen/pmu-tools.git
git clone https://github.com/brendangregg/FlameGraph

cd ${HOME}/fio
./configure
make

sudo sed -i 's/Defaults    requiretty/#Defaults    requiretty/g' /etc/sudoers
sudo setenforce 0
( awk '!/SELINUX=/' /etc/selinux/config ; echo "SELINUX=disabled" ) > /tmp/x
sudo mv /tmp/x /etc/selinux/config
rpm -qa firewalld | grep firewalld && sudo systemctl stop firewalld && sudo systemctl disable firewalld
sudo systemctl stop irqbalance
sudo systemctl disable irqbalance

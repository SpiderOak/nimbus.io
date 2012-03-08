#!/bin/bash

SRC_DIR=$HOME/nimbus_io_src
if [ ! -d $SRC_DIR ]; then mkdir $SRC_DIR ; fi

# everything that's new enough in apt
sudo apt-get -y install git wget
sudo apt-get -y install python-pip python-setuptools
sudo apt-get -y install mercurial
sudo apt-get -y install python-sphinx
sudo apt-get -y install python-jinja2
sudo apt-get -y install python-pygments
sudo apt-get -y install python-dev
sudo apt-get -y install cython
sudo apt-get -y install python-psycopg2 python-webob
sudo apt-get -y install python-zfec
sudo apt-get -y install postgresql-9.1
sudo apt-get -y build-dep zeromq

# HTTP domain for sphinx to build documentation (not in apt)
cd $SRC_DIR
if [ ! -d sphinx-contrib ]; then
    hg clone https://bitbucket.org/birkenfeld/sphinx-contrib
fi
cd sphinx-contrib/httpdomain
sudo python setup.py install

# build zeromq from source (we need newest for gevent-zeromq)
cd $SRC_DIR
if [ ! -e zeromq-2.1.11.tar.gz ]; then
    wget 'http://download.zeromq.org/zeromq-2.1.11.tar.gz'
fi
tar xzf zeromq-2.1.11.tar.gz
cd zeromq-2.1.11
./configure --prefix=/usr/local
make
sudo make install
sudo ldconfig

# install statgrabber from source (not in apt)
cd $SRC_DIR
if [ ! -e spideroak-statgrabber-1.0.2.tar.bz2 ]; then
    wget 'https://spideroak.com/dist/spideroak-statgrabber-1.0.2.tar.bz2'
fi
tar xjf spideroak-statgrabber-1.0.2.tar.bz2
cd spideroak-statgrabber-1.0.2/statgrabber
python setup.py build
sudo python setup.py install

# install the rest of the stuff from PyPI
sudo pip install --upgrade gevent
sudo pip install --upgrade greenlet
sudo pip install --upgrade pyzmq
sudo pip install --upgrade gevent_zeromq

# grab git src for all the nimbus code
cd $SRC_DIR
for pkg in lumberyard motoboto motoboto_benchmark
do 
    if [ ! -d $pkg ]; then
        git clone https://nimbus.io/dev/source/$pkg.git/
    fi
done

# install the Nimbus.io client libs from the git src we just grabbed
cd $SRC_DIR
for pkg in lumberyard motoboto
do
    pushd $pkg
    sudo python setup.py install
    popd
done

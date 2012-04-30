#!/bin/bash

# script to install all the necessary dependencies for Nimbus.IO on 
# Ubuntu Lucid.

SRC_DIR=$HOME/nimbus_io_src
CLEAN_EXISTING=0
GEVENT_FROM_SRC=0

if [ ! -d $SRC_DIR ]; then mkdir $SRC_DIR ; fi


set -e # stop on errors
set -x # echo every cmd

if [ "$CLEAN_EXISTING" ]; then
    sudo rm -vrf /usr/local/lib/python2.6/dist-packages/gevent* \
                 /usr/local/lib/python2.6/dist-packages/pyzmq* \
                 /usr/local/lib/python2.6/dist-packages/zmq*
fi


# these are new enough in the normal distribution
sudo apt-get update
sudo apt-get -y install libev3 libevent-dev m4 unifdef uuid-dev \
	build-essential python-setuptools python-dev python-crypto wget git-core \
    libc-ares-dev

# you need to add the new apt sources for either postgresql 9.0 or 9.1
# these are provided as a PPA by the maintainer of postgresql.
# details at https://launchpad.net/~pitti/+archive/postgresql

# these commands will set it up for you:

grep 'postgresql' /etc/apt/sources.list || {
	sudo bash -c 'echo "deb http://ppa.launchpad.net/pitti/postgresql/ubuntu lucid main" >> /etc/apt/sources.list'
	sudo bash -c 'echo "deb-src http://ppa.launchpad.net/pitti/postgresql/ubuntu lucid main" >> /etc/apt/sources.list'
    sudo apt-key adv --keyserver keyserver.ubuntu.com --recv-keys 8683D8A2
	sudo apt-get update
}
sudo apt-get install postgresql-9.1 libpq-dev


# build zeromq from source
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

sudo easy_install -UZ pip

if [ ! "$GEVENT_FROM_SRC" ]; then 
    sudo pip install --upgrade cython Mercurial greenlet gevent pyzmq psycopg2 webob
else
    # build gevent from most recent Mercurial source so we get c-ares support
    sudo pip install --upgrade cython Mercurial greenlet pyzmq psycopg2 webob
    cd $SRC_DIR
    if [ -e gevent ]; then sudo rm -rf gevent ; fi
    hg clone https://bitbucket.org/denis/gevent
    cd gevent
    python setup.py build 
    sudo python setup.py install
    cd $SRC_DIR
fi

# pull in these guys at specific version numbers b/c we need to be mindful of
# changes here.  pulls in argparse too.
sudo pip install --upgrade http://pypi.python.org/packages/source/p/pyutil/pyutil-1.8.4.tar.gz#md5=1eb14efa6184208a204a39e8e03d7354
sudo pip install --upgrade http://pypi.python.org/packages/source/z/zfec/zfec-1.4.22.tar.gz#md5=105745eb9d3db8f909786a0b39153a79

# had to manuallly rm gevent_zeromq/core.c to force it to get regenerated from
# the cython. Otherwise the constant sizes for zmq.core.socket.Socket wouldn't
# match.
# sudo pip install --upgrade gevent-zeromq

cd $SRC_DIR
if [ ! -e gevent_zeromq-0.2.2.tar.gz ]; then
    wget http://pypi.python.org/packages/source/g/gevent_zeromq/gevent_zeromq-0.2.2.tar.gz
fi
if [ -e gevent_zeromq-0.2.2 ]; then sudo rm -vrf gevent_zeromq-0.2.2 ; fi
tar xzf gevent_zeromq-0.2.2.tar.gz
cd gevent_zeromq-0.2.2 
rm -v gevent_zeromq/core.c
python setup.py build
sudo python setup.py install


# install statgrabber from source
cd $SRC_DIR
if [ ! -e spideroak-statgrabber-1.0.2.tar.bz2 ]; then
    wget 'https://spideroak.com/dist/spideroak-statgrabber-1.0.2.tar.bz2'
fi
tar xjf spideroak-statgrabber-1.0.2.tar.bz2
cd spideroak-statgrabber-1.0.2/statgrabber
python setup.py build
sudo python setup.py install

# only necessary for building the documentation
cd $SRC_DIR
sudo pip install --upgrade sphinx
if [ ! -d sphinx-contrib ]; then
    hg clone https://bitbucket.org/birkenfeld/sphinx-contrib
fi
cd sphinx-contrib/httpdomain
sudo python setup.py install

# grab git src for all the nimbus code
cd $SRC_DIR
for pkg in nimbus.io lumberyard motoboto motoboto_benchmark
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

# if we made it this far, all is good. 
echo Nimbus.io dependencies installed successfully.

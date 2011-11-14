#!/bin/bash

# script to install all the necessary dependencies for Nimbus.IO on Ubuntu
# Lucid.

set -e
set -x

# you need to add the new apt sources for either postgresql 9.0 or 9.1
# these are provided as a PPA by the maintainer of postgresql.
# details at https://launchpad.net/~pitti/+archive/postgresql

# these two commands will set it up for you:

#sudo bash -c 'echo "deb http://ppa.launchpad.net/pitti/postgresql/ubuntu lucid main" >> /etc/apt/sources.list'
#sudo bash -c 'echo "deb-src http://ppa.launchpad.net/pitti/postgresql/ubuntu lucid main" >> /etc/apt/sources.list'

sudo apt-get install postgresql-9.0

# these are new enough in the normal distribution
sudo apt-get install libev3 libevent-dev m4 unifdef 
mkdir ~/src
cd ~/src

# build zeromq from source
if [ ! -e zeromq-2.1.10.tar.gz ]; then
    wget 'http://download.zeromq.org/zeromq-2.1.10.tar.gz'
fi
tar xzf zeromq-2.1.10.tar.gz
cd zeromq-2.1.10
./configure --prefix=/usr/local
make
sudo make install
sudo ldconfig

sudo easy_install -UZ pip

sudo pip install --upgrade cython
sudo pip install --upgrade greenlet
sudo pip install --upgrade Mercurial

# pull in these guys at specific version numbers b/c we need to be mindful of
# changes here.  pulls in argparse too.
sudo pip install --upgrade http://pypi.python.org/packages/source/p/pyutil/pyutil-1.8.4.tar.gz#md5=1eb14efa6184208a204a39e8e03d7354
sudo pip install --upgrade http://pypi.python.org/packages/source/z/zfec/zfec-1.4.22.tar.gz#md5=105745eb9d3db8f909786a0b39153a79

sudo rm -vrf /usr/local/lib/python2.6/dist-packages/gevent* /usr/local/lib/python2.6/dist-packages/pyzmq* /usr/local/lib/python2.6/dist-packages/zmq*

# build gevent from most recent Mercurial source so we get c-ares support
sudo pip install --upgrade gevent
cd ~/src
#if [ -e gevent ]; then sudo rm -rf gevent ; fi
#hg clone https://bitbucket.org/denis/gevent
#cd gevent
#python setup.py build 
#sudo python setup.py install

sudo pip install --upgrade pyzmq

# had to manuallly rm gevent_zeromq/core.c to force it to get regenerated from
# the cython. Otherwise the constant sizes for zmq.core.socket.Socket wouldn't
# match.
# sudo pip install --upgrade gevent-zeromq

cd ~/src
if [ ! -e gevent_zeromq-0.0.4.tar.gz ]; then
    wget http://pypi.python.org/packages/source/g/gevent_zeromq/gevent_zeromq-0.0.4.tar.gz#md5=826853275fa025220136ea9fc59f6f1f
fi
if [ -e gevent_zeromq-0.0.4 ]; then sudo rm -vrf gevent_zeromq-0.0.4 ; fi
tar xzf gevent_zeromq-0.0.4.tar.gz
cd gevent_zeromq-0.0.4 
rm -v gevent_zeromq/core.c
python setup.py build
sudo python setup.py install


# need the newest psycopg2 that supports non-blocking callbacks
sudo pip install --upgrade psycopg2

# install statgrabber from source
cd ~/src
if [ ! -e spideroak-statgrabber-1.0.2.tar.bz2 ]; then
    wget 'https://spideroak.com/dist/spideroak-statgrabber-1.0.2.tar.bz2'
fi
tar xjf spideroak-statgrabber-1.0.2.tar.bz2
cd ~/src/spideroak-statgrabber-1.0.2/statgrabber
python setup.py build
sudo python setup.py install


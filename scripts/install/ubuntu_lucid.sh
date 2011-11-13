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


# build gevent from most recent source so we get c-ares
cd ~/src
hg clone https://bitbucket.org/denis/gevent
cd gevent
python setup.py build 
sudo python setup.py install

sudo pip install --upgrade pyzmq
sudo pip install --upgrade gevent-zeromq
sudo pip install --upgrade psycopg2



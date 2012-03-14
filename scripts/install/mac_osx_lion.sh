#!/bin/bash

# script to install all the necessary dependencies for Nimbus.IO on 
# Mac OS X Lion

GEVENT_FROM_SRC=0
CLEAN_EXISTING=0

set -e # stop on errors
set -x # echo every cmd

if [ "$CLEAN_EXISTING" ]; then
    rm -vrf /Library/Python/2.7/site-packages/gevent* \
            /Library/Python/2.7/site-packages/pyzmq* \
            /Library/Python/2.7/site-packages/cython* \
            /Library/Python/2.7/site-packages/Cython* \
            /Library/Python/2.7/site-packages/pyzmq
fi

# first, you need to have homebrew installed.
# It's super easy and gives you a package manager to so 
# many unix tools and libraries.  http://mxcl.github.com/homebrew/

# have brew install these dependencies for you
brew install c-ares 
brew install libevent 
brew install libev 
brew install zeromq 
brew install postgresql

if [ ! -d ~/src ]; then mkdir ~/src fi
cd ~/src

sudo easy_install -UZ pip
sudo pip install --upgrade \
	http://pypi.python.org/packages/source/p/pyutil/pyutil-1.8.4.tar.gz#md5=1eb14efa6184208a204a39e8e03d7354 \
	http://pypi.python.org/packages/source/z/zfec/zfec-1.4.22.tar.gz#md5=105745eb9d3db8f909786a0b39153a79 \

sudo pip install Mercurial
sudo pip install --upgrade cython

if [ ! "$GEVENT_FROM_SRC" ]; then 
    # just install stable gevent. this doesn't use the c-ares lib, and can't
    # resolve entries from /etc/hosts

    # note that we have tried to make this easy to work around, by making the
    # DNS servers for the nimbus.io domain answer every query matching
    # *.sim.nimbus.io with 127.0.0.1, so even without /etc/hosts resolution you
    # can at least run a dev simulated cluster on localhost.

    sudo pip install --upgrade greenlet gevent pyzmq psycopg2 webob

else
    sudo pip install --upgrade greenlet pyzmq psycopg2 webob

    # if you just intsall stable gevent, you get the one that does not use
    # c-ares for async dns resolution. that means it won't be able to read
    # /etc/hosts.  so we recommend using the newest gevent source.  pip install
    # gevent

    # install gevent from source. 
    # requires also a minimal patch to compile on Lion (that's the vim line below.) 
    # You can try it without it. It maybe fixed by now.

    pushd ~/src
    hg clone https://bitbucket.org/denis/gevent
    cd ~/src/gevent
    vim libev/ev.c +885  # remove this line:
    # error "memory fences not defined for your architecture, please report"
    python setup.py build && sudo python setup.py install
    popd
fi

# had to manuallly rm gevent_zeromq/core.c to force it to get regenerated from
# the cython. Otherwise the constant sizes for zmq.core.socket.Socket wouldn't
# match.
# sudo pip install --upgrade gevent-zeromq
cd ~/src
if [ ! -e gevent_zeromq-0.2.0.tar.gz ]; then
    wget http://pypi.python.org/packages/source/g/gevent_zeromq/gevent_zeromq-0.2.0.tar.gz#md5=0577be386a3d46451954010688cb50f8
fi
if [ -e gevent_zeromq-0.2.0 ]; then sudo rm -vrf gevent_zeromq-0.2.0 ; fi
tar xzf gevent_zeromq-0.2.0.tar.gz
cd gevent_zeromq-0.2.0 
rm -v gevent_zeromq/core.c
python setup.py build
sudo python setup.py install

# install statgrabber from source
cd ~/src
if [ ! -e spideroak-statgrabber-1.0.2.tar.bz2 ]; then
    wget 'https://spideroak.com/dist/spideroak-statgrabber-1.0.2.tar.bz2'
fi
tar xjf spideroak-statgrabber-1.0.2.tar.bz2
cd ~/src/spideroak-statgrabber-1.0.2/statgrabber
python setup.py build
sudo python setup.py install

# only necessary for building the documentation
sudo pip install --upgrade sphinx
cd ~/src
if [ ! -d sphinx-contrib ]; then
    hg clone https://bitbucket.org/birkenfeld/sphinx-contrib
fi
cd sphinx-contrib/httpdomain
sudo python setup.py install





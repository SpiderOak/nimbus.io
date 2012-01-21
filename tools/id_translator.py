#!/usr/bin/env python

"""
Nimbus.io uses unified ID numbers for a variety of purposes -- most notably
version IDs.  It is convenient to provide those same identifiers to users to
allow them to specify particular objects to perform operations on.  

However, exposing internal IDs directly also leaks some information.  For
example, if the ID numbers were simply sequentially generated (they are not) it
becomes trivial for end users to determine the rate at which objects are being
added to the system.  Comparisons can be made among IDs to determine operation
ordering.  It also facilitates a variety of forms of abuse that involve
specifying made up IDs.

The scheme here provides some rudimentary protection against the above.  

Internal IDs maybe translated to an encrypted form and freely shared with the
public.  The public form does not reveal the internal ID, public IDs are not
comparable to determine operation order, and the use of HMAC offers some
limited protection for erroneous public IDs being made up.

Note of course that that none of these protections imply that a user who knows
a particular public ID has rights to any object.  The only (limited) protection
implied is that a verified public ID is likely to have originated within our
system.

Within Nimbus.io, one set of keys (such as to instantiate a
InternalIDTranslator) should be used per organization operating a Nimbus.io
service.  Naturally the keys should be kept secret.

When using the cluster simulation infrastructure, keys for new clusters are
automatically generated when the simulated cluster is created.
"""

import os
import time
import struct
import hmac
import base64
from itertools import izip, chain
from hashlib import sha256
from Crypto.Cipher import AES

MIN_ID = 1
MAX_ID = (2 ** 64) - 1

_KEY_SIZE = 32
_IV_SIZE = 16

def sec_str_eq(str1, str2):
    """
    constant time string comparison for arbitrary strings
    """

    if not len(str1) == len(str2):
        return False
    
    match_count = 0
    for a, b in izip(str1, str2):
        match_count += 1 if a == b else 0

    return match_count == len(str1)
            
def int8_to_bin(int8):
    "pack int8 to binary string"
    return struct.pack("!Q", int8)
 
def bin_to_int8(bin_str):
    "unpack int8 from binary string"
    return struct.unpack("!Q", bin_str)[0]
    
def check_range(internal_id):
    "sanity check"
    if internal_id < MIN_ID or internal_id > MAX_ID:
        raise ValueError("Bad ID")

class InternalIDTranslator(object):
    """
    class for safely translating internal IDs to public IDs and back without
    info leaks
    """

    def __init__(self, key, iv_key, hmac_key, hmac_size=16):

        if not len(key) == len(iv_key) == len(hmac_key) == _KEY_SIZE:
            raise ValueError("incorrect key sizes")

        if not len(set([key, iv_key, hmac_key, ])) == 3:
            raise ValueError("don't use equal keys")

        if not hmac_size <= _KEY_SIZE:
            raise ValueError("hmac size out of range")

        self.key = key
        self.iv_key = iv_key
        self.hmac_key = hmac_key
        self.hmac_size = hmac_size

    def _make_iv(self, bin_str):
        "create an initial vector for aes"
        return hmac.new(self.iv_key, bin_str, sha256).digest()[:_IV_SIZE]

    def _make_hmac(self, signtext):
        "make a verification code of desired width"
        return hmac.new(
            self.hmac_key, signtext, sha256).digest()[:self.hmac_size]


    def public_id(self, internal_id):
        """
        create a public version of an internal 8 byte integer
        """
        check_range(internal_id)
        binary_id = int8_to_bin(internal_id)
        aes_iv = self._make_iv(binary_id)
        ciphertxt = AES.new(self.key, AES.MODE_CFB, aes_iv).encrypt(binary_id)
        hmactxt = self._make_hmac(aes_iv + ciphertxt)
        b64txt = base64.urlsafe_b64encode(aes_iv + hmactxt + ciphertxt)
        return b64txt.rstrip('=')

    def internal_id(self, public_id):
        """
        decode a public id and return the internal ID
        will raise ValueError on corrupted IDs
        """

        # add enough padding to make base64 happy
        for n in range(3):
            try:
                padding = n * '='
                binary = base64.urlsafe_b64decode(public_id + padding)
                break
            except TypeError, err:
                if not "padding" in str(err):
                    raise ValueError("Bad ID")
        else: 
            raise ValueError("Bad ID")

        aes_iv = binary[0:_IV_SIZE]
        hmac_txt = binary[_IV_SIZE:_IV_SIZE + self.hmac_size]
        ciphertxt = binary[_IV_SIZE + self.hmac_size:]

        correct_hmac_txt = self._make_hmac(aes_iv + ciphertxt)

        if not len(hmac_txt) == self.hmac_size:
            raise ValueError("Bad ID")

        if not sec_str_eq(hmac_txt, correct_hmac_txt):
            raise ValueError("Bad ID")

        plaintxt = AES.new(self.key, AES.MODE_CFB, aes_iv).decrypt(ciphertxt)
        if not len(plaintxt) == 8:
            raise ValueError("Bad ID")

        internal_id = bin_to_int8(plaintxt)

        check_range(internal_id)

        return internal_id

def test(rounds=10000):
    "test translating IDs"

    key = os.urandom(_KEY_SIZE)
    hmac_key = os.urandom(_KEY_SIZE)
    iv_key = os.urandom(_KEY_SIZE)

    translator = InternalIDTranslator(key, hmac_key, iv_key, 16)

    timestart = int(time.time() * 1000)

    for test_id in chain(
        range(MIN_ID, MIN_ID + rounds),
        range(timestart, timestart + rounds),
        range(MAX_ID, MAX_ID - rounds, -1)
    ):
        public = translator.public_id(test_id)
        print public, len(public)
        internal = translator.internal_id(public)
        assert test_id == internal, (test_id, internal,)

    return True

if __name__ == "__main__":
    test()

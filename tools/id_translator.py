#!/usr/bin/python -w

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
    constant time string comparison for arbirtrary strings
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
    
class InternalIDTranslator(object):
    """
    class for safely translating internal IDs to public IDs and back without
    info leaks
    """

    def __init__(self, key, iv_key, hmac_key, hmac_size=16):
        self.key = key
        self.iv_key = iv_key
        self.hmac_key = hmac_key
        self.hmac_size = hmac_size
        if not len(key) == len(iv_key) == len(hmac_key) == _KEY_SIZE:
            raise ValueError("incorrect key sizes")
        assert self.hmac_size <= _KEY_SIZE

    def _make_iv(self, bin_str):
        "create an initial vector for aes"
        return hmac.new(self.iv_key, bin_str, sha256).digest()[:_IV_SIZE]

    def _make_hmac(self, signtext):
        "make a verification code of desired width"
        return hmac.new(
            self.hmac_key, signtext, sha256).digest()[:self.hmac_size]

    def _check_range(self, internal_id):
        "sanity check"
        if internal_id < MIN_ID or internal_id > MAX_ID:
            raise ValueError("Bad ID")

    def public_id(self, internal_id):
        """
        create a public version of an internal 8 byte integer
        """
        self._check_range(internal_id)
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
                    raise 
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

        self._check_range(internal_id)

        return internal_id

def test():
    "test translating IDs"
    key = os.urandom(_KEY_SIZE)
    hmac_key = os.urandom(_KEY_SIZE)
    iv_key = os.urandom(_KEY_SIZE)

    translator = InternalIDTranslator(key, hmac_key, iv_key, 16)

    start = int(time.time() * 1000)

    for test_id in chain(
        xrange(start, start + 1000),
        xrange(MAX_ID, MAX_ID - 1000, -1)
    ):
        public = translator.public_id(test_id)
        print public, len(public)
        internal = translator.internal_id(public)
        assert test_id == internal, (test_id, internal,)

if __name__ == "__main__":
    test()

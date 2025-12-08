import base64
import json
from Crypto.Cipher import AES
from Crypto.Util.Padding import pad, unpad


# AES utilities for CP <-> Central encrypted messages

def aes_encrypt(sym_key_hex: str, plaintext: str) -> str:
    """
    Encrypts plaintext using AES-256-CBC with a random IV.
    Returns base64(iv + ciphertext).
    """
    key = bytes.fromhex(sym_key_hex)
    cipher = AES.new(key, AES.MODE_CBC)
    ct = cipher.encrypt(pad(plaintext.encode(), AES.block_size))
    out = cipher.iv + ct
    return base64.b64encode(out).decode()


def aes_decrypt(sym_key_hex: str, encrypted_b64: str) -> str:
    """
    Decrypts base64(iv+ciphertext) to plaintext.
    """
    raw = base64.b64decode(encrypted_b64)
    iv = raw[:16]
    ct = raw[16:]
    key = bytes.fromhex(sym_key_hex)
    cipher = AES.new(key, AES.MODE_CBC, iv)
    pt = unpad(cipher.decrypt(ct), AES.block_size)
    return pt.decode()

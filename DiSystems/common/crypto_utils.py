from Crypto.Cipher import AES
from Crypto.Random import get_random_bytes
import base64
import json

def aes_encrypt(sym_key_hex: str, plaintext: str) -> str:
    key = bytes.fromhex(sym_key_hex)
    iv = get_random_bytes(16)

    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded = plaintext + (16 - len(plaintext) % 16) * chr(16 - len(plaintext) % 16)
    ciphertext = cipher.encrypt(padded.encode())

    msg = {
        "iv": base64.b64encode(iv).decode(),
        "data": base64.b64encode(ciphertext).decode()
    }
    return json.dumps(msg)

def aes_decrypt(sym_key_hex: str, msg_json: str) -> str:
    key = bytes.fromhex(sym_key_hex)

    obj = json.loads(msg_json)
    iv = base64.b64decode(obj["iv"])
    ciphertext = base64.b64decode(obj["data"])

    cipher = AES.new(key, AES.MODE_CBC, iv)
    padded = cipher.decrypt(ciphertext).decode()

    pad_len = ord(padded[-1])
    return padded[:-pad_len]

import json

def encrypt_kafka_payload(sym_key: str, data: dict) -> str:
    """
    Encrypts a Kafka message payload (dict) using AES.
    Returns a base64-encoded ciphertext string.
    """
    plaintext = json.dumps(data)
    return aes_encrypt(sym_key, plaintext)


def decrypt_kafka_payload(sym_key: str, ciphertext: str) -> dict:
    """
    Decrypts a Kafka message payload encrypted with AES.
    Returns the original dict.
    """
    plaintext = aes_decrypt(sym_key, ciphertext)
    return json.loads(plaintext)


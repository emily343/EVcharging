# protocol_utils.py
STX = b'\x02'
ETX = b'\x03'
ACK = b'\x06'
NACK = b'\x15'

def compute_lrc(data: bytes) -> bytes:
    lrc = 0
    for b in data:
        lrc ^= b
    return bytes([lrc])

def pack_message(payload_str: str) -> bytes:
    payload = payload_str.encode('utf-8')
    lrc = compute_lrc(payload)
    return STX + payload + ETX + lrc

def unpack_message(raw: bytes):
    """
    raw: bytes containing STX..ETX..LRC
    Returns tuple (payload_str or None, valid_lrc_bool)
    """
    try:
        if not raw or raw[0:1] != STX:
            return None, False
        etx_index = raw.index(ETX)
        payload = raw[1:etx_index]
        lrc_received = raw[etx_index+1:etx_index+2]
        lrc_calc = compute_lrc(payload)
        payload_str = payload.decode('utf-8', errors='ignore').strip()
        return payload_str, lrc_calc == lrc_received
    except Exception:
        return None, False

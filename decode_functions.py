import socket
import xml.etree.ElementTree as ET
from datetime import datetime
from datetime import time

from typing import Dict, Any
from datetime import datetime
from typing import Optional




def decode_int(value: bytes):
    
    value1 =  int.from_bytes(value, byteorder='big')
    #print("int",value,value1)
    return str(value1)


def decode_bcd_time(raw: bytes) -> str:
    """
    Decode ASN.1 BCD encoded Time with optional milliseconds.
    Supports:
    - 3 bytes: hhmmss
    - 2 bytes: hhmm
    - 4 bytes: hhmmssSS (SS = hundredths)
    - 5 bytes: hhmmssSSS (SSS = milliseconds)
    """
    if not raw:
        return None
 
    digits = []
    for byte in raw:
        hi = (byte >> 4) & 0x0F
        lo = byte & 0x0F
        digits.append(str(hi))
        digits.append(str(lo))
 
    numstr = "".join(digits)
 
    try:
        if len(raw) == 2:   # hhmm
            t = time(int(numstr[0:2]), int(numstr[2:4]))
            return t.strftime("%H:%M")
 
        elif len(raw) == 3: # hhmmss
            t = time(int(numstr[0:2]), int(numstr[2:4]), int(numstr[4:6]))
            return t.strftime("%H:%M:%S")
 
        elif len(raw) == 4: # hhmmssSS (hundredths)
            t = time(int(numstr[0:2]), int(numstr[2:4]), int(numstr[4:6]),
                     int(numstr[6:8]) * 10000)  # hundredths → microseconds
            return t.strftime("%H:%M:%S.%f")[:-4]  # show 2 decimals
 
        elif len(raw) == 5: # hhmmssSSS (milliseconds)
            t = time(int(numstr[0:2]), int(numstr[2:4]), int(numstr[4:6]),
                     int(numstr[6:9]) * 1000)  # ms → microseconds
            return t.strftime("%H:%M:%S.%f")[:-3]  # show 3 decimals
 
        else:
            return numstr  # unknown format, return digits
 
    except Exception:
        return numstr



def decode_octet_time(raw: bytes) -> str:
    """
    Decode time from unsigned OCTET STRING.
    Supports:
    - 3 bytes: HH MM SS
    - 4 bytes: HH MM SS MS (milliseconds)
    """
    if not raw or len(raw) < 3:
        raise ValueError("Expected at least 3 bytes for time")

    hour = raw[0]
    minute = raw[1]
    second = raw[2]
    millisecond = raw[3] if len(raw) >= 4 else 0

    # Convert milliseconds to microseconds (datetime.time expects microseconds)
    microsecond = millisecond * 1000

    t = time(hour, minute, second, microsecond)
    return t.strftime("%H:%M:%S.%f")[:-3]  # Trim to milliseconds

def decode_octet_string(raw: bytes) -> str:
    """
    Convert an unsigned OCTET STRING into a  string of decimal values.
    
    Example:
    b'\x17\x0c\x0c' → '231212'
    """
    if not raw:
        return ""
    
    return "".join(str(byte) for byte in raw)




def decode_octet_datetime(raw: bytes) -> str:
    if not raw or len(raw) < 3:
        raise ValueError("Insufficient bytes for date decoding")

    try:
        if len(raw) == 3:
            year = 2000 + raw[0]
            month = raw[1]
            day = raw[2]
            dt = datetime(year, month, day)

        elif len(raw) >= 4:
            year = int.from_bytes(raw[0:2], byteorder='big')  # 2-byte year
            month = raw[2]
            day = raw[3]

            hour = raw[4] if len(raw) >= 5 else 0
            minute = raw[5] if len(raw) >= 6 else 0
            second = raw[6] if len(raw) >= 7 else 0

            dt = datetime(year, month, day, hour, minute, second)
        #print(dt.strftime("%Y-%m-%d %H:%M:%S") if len(raw) > 4 else dt.strftime("%Y-%m-%d"))
        return dt.strftime("%Y-%m-%d %H:%M:%S") if len(raw) > 4 else dt.strftime("%Y-%m-%d")

    except Exception as e:
        return f"❌ Invalid date: {e}"

def decode_bcd_datetime(raw: bytes) -> Optional[str]:
    """
    Decode ASN.1 BCD-encoded date or datetime.
    Supports:
    - 3 bytes: YYMMDD
    - 4 bytes: YYYYMMDD
    - 6 bytes: YYMMDDhhmmss
    - 7 bytes: YYYYMMDDhhmmss
    """
    if not raw:
        return None

    # Convert each nibble to a digit string
    digits = ''.join(
        f"{(byte >> 4) & 0x0F}{byte & 0x0F}"
        for byte in raw
    )

    try:
        format_map = {
            3: ("%y%m%d", "%Y-%m-%d"),
            4: ("%Y%m%d", "%Y-%m-%d"),
            6: ("%y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S"),
            7: ("%Y%m%d%H%M%S", "%Y-%m-%d %H:%M:%S")
        }

        if len(raw) in format_map:
            input_fmt, output_fmt = format_map[len(raw)]
            dt = datetime.strptime(digits[:len(input_fmt)], input_fmt)
            return dt.strftime(output_fmt)

        return digits  # fallback: return raw digits

    except Exception:
        return digits  # fallback on parse failure



def decode_smsc_date(raw: bytes) -> Optional[datetime]:
    """
    Decode a TBCD-encoded timestamp from SMSC logs into a Python datetime.
    Example raw input: b'\x62\x10\x91\x80\x90\x42\x00'
    Format: YYMMDDhhmmss + optional ms (last byte often '00').
    """
    #print(raw)
    # Decode each byte: low nibble first, then high nibble
    digits = []
    for b in raw:
        lo = b & 0x0F
        hi = (b & 0xF0) >> 4
        digits.append(str(lo))
        digits.append(str(hi))

    digit_str = ''.join(digits)
    # First 12 digits = YYMMDDhhmmss
    yy = int(digit_str[0:2])
    mm = int(digit_str[2:4])
    dd = int(digit_str[4:6])
    hh = int(digit_str[6:8])
    mi = int(digit_str[8:10])
    ss = int(digit_str[10:12])

    # Handle year rollover (spec usually means 2000+)
    year = 2000 + yy if yy < 95 else 1900 + yy

    # Milliseconds (optional, last two digits)
    ms = int(digit_str[12:14]) if len(digit_str) >= 14 else 0

    dt = datetime(year, mm, dd, hh, mi, ss)
    return dt.strftime("%Y-%m-%d-%H%M%S")




def decode_tbcd_string(data: bytes) -> str:
    digits = []
    
    for byte in data:
        low = byte & 0x0F
        high = (byte & 0xF0) >> 4

        # Append low nibble first
        if low != 0xF:
            digits.append(str(low))
        # Then high nibble
        if high != 0xF:
            digits.append(str(high))

    #print("I am in tbcd string",data.hex(),digits)
    return ''.join(digits)

def decode_bcd_string(data: bytes) -> str:
    return data.hex()




def decode_ip(data):
    if isinstance(data, bytes) and len(data) == 4:
        return socket.inet_ntoa(data)
    elif isinstance(data, bytes) and len(data) == 16:
        return socket.inet_ntop(socket.AF_INET6, data)
    return decode_octet_string(data)

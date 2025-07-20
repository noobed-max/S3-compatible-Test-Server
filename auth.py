import hashlib
import hmac
from datetime import datetime
from typing import Mapping, Union, List, Tuple
from urllib.parse import unquote, parse_qsl

from fastapi import Depends, HTTPException, Request
from sqlalchemy.orm import Session

import crud
from database import get_db

def _get_canonical_headers(headers: Mapping[str, str]) -> tuple[str, str]:
    ordered_headers = {k.lower(): v for k, v in headers.items()}
    signed_headers = ";".join(sorted(ordered_headers.keys()))
    canonical_headers = "\n".join(
        f"{k}:{v}" for k, v in sorted(ordered_headers.items())
    )
    return canonical_headers, signed_headers

def _get_canonical_request(request: Request, signed_headers: str, payload_hash: str) -> str:
    method = request.method
    path = unquote(request.url.path)
    query_params = parse_qsl(request.url.query, keep_blank_values=True)
    
    # Sort the query parameters by key
    query_params.sort(key=lambda x: x[0])
    
    # Reconstruct the canonical query string. Example: 'uploads='
    query = "&".join([f"{k}={v}" for k, v in query_params])
    
    # Extract only signed headers for the canonical request
    canonical_headers_dict = {
        k.lower(): v for k,v in request.headers.items() if k.lower() in signed_headers.split(";")
    }
    canonical_headers = "\n".join(
        f"{k}:{v}" for k, v in sorted(canonical_headers_dict.items())
    )
    
    return f"{method}\n{path}\n{query}\n{canonical_headers}\n\n{signed_headers}\n{payload_hash}"

def _get_string_to_sign(canonical_request_hash: str, timestamp: str, scope: str) -> str:
    return f"AWS4-HMAC-SHA256\n{timestamp}\n{scope}\n{canonical_request_hash}"

def _get_signing_key(secret_key: str, date_stamp: str, region: str, service: str) -> bytes:
    k_date = hmac.new(f"AWS4{secret_key}".encode(), date_stamp.encode(), hashlib.sha256).digest()
    k_region = hmac.new(k_date, region.encode(), hashlib.sha256).digest()
    k_service = hmac.new(k_region, service.encode(), hashlib.sha256).digest()
    k_signing = hmac.new(k_service, b"aws4_request", hashlib.sha256).digest()
    return k_signing

async def get_current_user(request: Request, db: Session = Depends(get_db)):
    auth_header = request.headers.get("authorization")
    if not auth_header or not auth_header.startswith("AWS4-HMAC-SHA256"):
        raise HTTPException(status_code=403, detail="Invalid authorization header")

    # Safely split the header into algorithm and the rest of the credentials
    auth_parts = auth_header.split(" ", 1)
    if len(auth_parts) != 2:
        # Handles cases where the header is just "AWS4-HMAC-SHA256" with no credentials
        raise HTTPException(
            status_code=403, detail="Malformed authorization header: Missing credentials."
        )

    credential_string = auth_parts[1]

    try:
        parts = {
            p.split("=")[0].strip(): p.split("=")[1].strip()
            for p in credential_string.split(",")
        }
        credential = parts["Credential"]
        signed_headers = parts["SignedHeaders"]
        signature = parts["Signature"]

        access_key, date_stamp, region, service, _ = credential.split("/")
    except (ValueError, KeyError, IndexError) as e:
        raise HTTPException(status_code=403, detail=f"Malformed authorization header parts: {e}")

    user = crud.get_user_by_access_key(db, access_key)
    if not user:
        raise HTTPException(status_code=403, detail="Invalid access key")

    timestamp = request.headers.get("x-amz-date")
    payload_hash = request.headers.get("x-amz-content-sha256")

    canonical_request = _get_canonical_request(request, signed_headers, payload_hash)
    canonical_request_hash = hashlib.sha256(canonical_request.encode()).hexdigest()

    scope = f"{date_stamp}/{region}/{service}/aws4_request"
    string_to_sign = _get_string_to_sign(canonical_request_hash, timestamp, scope)

    signing_key = _get_signing_key(user.secret_key, date_stamp, region, service)

    calculated_signature = hmac.new(
        signing_key, string_to_sign.encode(), hashlib.sha256
    ).hexdigest()

    if calculated_signature != signature:
        raise HTTPException(status_code=403, detail="Signature does not match")

    return user
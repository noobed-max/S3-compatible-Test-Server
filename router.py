import uuid
import xml.etree.ElementTree as ET
from fastapi import APIRouter, Depends, Request, Response, HTTPException
from sqlalchemy.orm import Session
from auth import get_current_user
from database import get_db
import crud
import models
import storage
from responses import (
    generate_error_response,
    initiate_multipart_upload_response,
    complete_multipart_upload_response,
    generate_location_response,
    generate_list_objects_v2_response,
)

router = APIRouter()

@router.get("/{bucket_name}/")
@router.get("/{bucket_name}")
def get_bucket(
    bucket_name: str,
    request: Request,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """
    Handles GET requests on a bucket.
    Differentiates between GetBucketLocation and ListObjects based on query params.
    """
    # Check if this is a GetBucketLocation request
    bucket = crud.get_bucket_by_name(db, name=bucket_name)
    if not bucket or bucket.owner_id != current_user.id:
        error_xml = generate_error_response("NoSuchBucket", "The specified bucket does not exist.", f"/{bucket_name}")
        return Response(content=error_xml, media_type="application/xml", status_code=404)

    # Handle GetBucketLocation
    if "location" in request.query_params:
        xml_response = generate_location_response()
        return Response(content=xml_response, media_type="application/xml")

    # Handle ListObjectsV2
    if "list-type" in request.query_params and request.query_params["list-type"] == "2":
        prefix = request.query_params.get("prefix", "")
        max_keys = int(request.query_params.get("max-keys", 1000))
        continuation_token = request.query_params.get("continuation-token")

        objects, is_truncated, next_token = crud.list_objects(
            db,
            bucket_id=bucket.id,
            prefix=prefix,
            marker=continuation_token,
            limit=max_keys,
        )

        xml_response = generate_list_objects_v2_response(
            bucket_name=bucket.name,
            prefix=prefix,
            marker=continuation_token,
            max_keys=max_keys,
            is_truncated=is_truncated,
            objects=objects,
            next_marker=next_token,
        )
        return Response(content=xml_response, media_type="application/xml")

    # Fallback for other unimplemented GET bucket operations
    return Response(
        content=generate_error_response("NotImplemented", "The requested bucket operation is not implemented.", f"/{bucket_name}"),
        media_type="application/xml",
        status_code=501
    )


@router.head("/{bucket_name}/")
@router.head("/{bucket_name}")
def head_bucket(bucket_name: str, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    bucket = crud.get_bucket_by_name(db, name=bucket_name)
    if not bucket or bucket.owner_id != current_user.id:
        return Response(status_code=404)
    return Response(status_code=200)

@router.put("/{bucket_name}/")
@router.put("/{bucket_name}")
def create_bucket(bucket_name: str, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    if crud.get_bucket_by_name(db, name=bucket_name):
        error_xml = generate_error_response("BucketAlreadyOwnedByYou", "Your previous request to create the named bucket succeeded and you already own it.", f"/{bucket_name}")
        return Response(content=error_xml, media_type="application/xml", status_code=409)
    
    crud.create_bucket(db, name=bucket_name, owner_id=current_user.id)
    storage.create_bucket_folder(bucket_name)
    return Response(status_code=200)

@router.head("/{bucket_name}/{object_name:path}")
def head_object(
    bucket_name: str,
    object_name: str,
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """Handles HEAD requests for an object to retrieve metadata."""
    bucket = crud.get_bucket_by_name(db, name=bucket_name)
    if not bucket or bucket.owner_id != current_user.id:
        return Response(status_code=404, content=generate_error_response("NoSuchBucket", "The specified bucket does not exist.", f"/{bucket_name}"))

    db_object = crud.get_object_by_bucket_and_name(db, bucket_id=bucket.id, name=object_name)
    if not db_object:
        return Response(status_code=404, content=generate_error_response("NoSuchKey", "The specified key does not exist.", f"/{bucket_name}/{object_name}"))

    headers = {
        "ETag": f'"{db_object.etag}"',
        "Content-Length": str(db_object.size),
        "Content-Type": db_object.content_type,
        "Last-Modified": db_object.last_modified.strftime("%a, %d %b %Y %H:%M:%S GMT"),
    }
    return Response(status_code=200, headers=headers)

@router.post("/{bucket_name}/{object_name:path}")
async def multipart_actions(bucket_name: str, object_name: str, request: Request, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    if "uploads" in request.query_params:
        # Initiate Multipart Upload
        upload_id = str(uuid.uuid4())
        crud.create_multipart_upload(db, upload_id=upload_id, bucket_name=bucket_name, object_name=object_name)
        xml_response = initiate_multipart_upload_response(bucket_name, object_name, upload_id)
        return Response(content=xml_response, media_type="application/xml")
    
    if "uploadId" in request.query_params:
        # Complete Multipart Upload
        upload_id = request.query_params["uploadId"]
        upload = crud.get_multipart_upload(db, upload_id)
        if not upload:
            raise HTTPException(status_code=404, detail="Upload not found")
        
        body = await request.body()
        xml_body = ET.fromstring(body)

        namespace = {'s3': xml_body.tag.split('}')[0].strip('{')}

        # Verify parts from request body against DB using the namespace
        client_parts = {
            int(p.find('s3:PartNumber', namespace).text): p.find('s3:ETag', namespace).text.strip('"')
            for p in xml_body.findall("s3:Part", namespace)
        }
        
        db_parts = sorted(upload.parts, key=lambda p: p.part_number)
        if len(client_parts) != len(db_parts) or any(client_parts[p.part_number] != p.etag for p in db_parts):
             raise HTTPException(status_code=400, detail="Invalid parts list")

        size, etag = storage.combine_parts(bucket_name, object_name, db_parts)
        bucket = crud.get_bucket_by_name(db, bucket_name)
        
        crud.create_object(db, bucket_id=bucket.id, name=object_name, size=size, etag=etag, filepath=str(storage.STORAGE_ROOT / bucket_name / object_name), content_type="application/octet-stream")
        crud.delete_multipart_upload(db, upload_id)
        
        location = f"http://{request.headers['host']}/{bucket_name}/{object_name}"
        xml_response = complete_multipart_upload_response(bucket_name, object_name, etag, location)
        return Response(content=xml_response, media_type="application/xml")

    raise HTTPException(status_code=400, detail="Invalid request")


@router.put("/{bucket_name}/{object_name:path}")
async def put_object(bucket_name: str, object_name: str, request: Request, db: Session = Depends(get_db), current_user: models.User = Depends(get_current_user)):
    bucket = crud.get_bucket_by_name(db, name=bucket_name)
    if not bucket or bucket.owner_id != current_user.id:
        raise HTTPException(status_code=404, detail="Bucket not found")

    content_type = request.headers.get("content-type", "application/octet-stream")
    body = await request.body()

    if "uploadId" in request.query_params and "partNumber" in request.query_params:
        # Upload Part
        upload_id = request.query_params["uploadId"]
        part_number = int(request.query_params["partNumber"])
        
        upload = crud.get_multipart_upload(db, upload_id)
        if not upload or upload.bucket_name != bucket_name or upload.object_name != object_name:
            raise HTTPException(status_code=404, detail="Upload ID not found for this object.")

        filepath, etag = storage.save_part(upload_id, part_number, body)
        crud.create_multipart_part(db, upload_id=upload_id, part_number=part_number, etag=etag, filepath=filepath)
        
        return Response(headers={"ETag": f'"{etag}"'})

    # Single part upload
    size, etag = storage.save_object(bucket_name, object_name, body)
    crud.create_object(db, bucket_id=bucket.id, name=object_name, size=size, etag=etag, filepath=str(storage.STORAGE_ROOT / bucket_name / object_name), content_type=content_type)
    
    return Response(headers={"ETag": f'"{etag}"'})
@router.delete("/{bucket_name}/{object_name:path}")
def abort_multipart_upload(
    bucket_name: str,
    object_name: str,
    uploadId: str, # FastAPI gets query params by name
    db: Session = Depends(get_db),
    current_user: models.User = Depends(get_current_user)
):
    """Handles aborting a multipart upload."""
    upload = crud.get_multipart_upload(db, uploadId)
    if not upload or upload.bucket_name != bucket_name or upload.object_name != object_name:
        raise HTTPException(status_code=404, detail="The specified multipart upload does not exist.")

    # 1. Clean up stored part files from the disk
    storage.cleanup_parts(uploadId)
    
    # 2. Delete the upload record from the database
    crud.delete_multipart_upload(db, uploadId)

    # 3. Return the correct success response
    return Response(status_code=204) # 204 No Content
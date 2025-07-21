from sqlalchemy.orm import Session
import models
from sqlalchemy import asc
def get_user_by_access_key(db: Session, access_key: str):
    return db.query(models.User).filter(models.User.access_key == access_key).first()

def get_bucket_by_name(db: Session, name: str):
    return db.query(models.Bucket).filter(models.Bucket.name == name).first()

def get_object_by_bucket_and_name(db: Session, bucket_id: int, name: str):
    """Fetches an object from the database by its bucket and name."""
    return db.query(models.Object).filter(
        models.Object.bucket_id == bucket_id,
        models.Object.name == name
    ).first()
def list_objects(db: Session, bucket_id: int, prefix: str, marker: str, limit: int):
    """Lists objects in a bucket with pagination."""
    query = db.query(models.Object).filter(models.Object.bucket_id == bucket_id)
    
    if prefix:
        query = query.filter(models.Object.name.startswith(prefix))
        
    if marker:
        query = query.filter(models.Object.name > marker)
        
    objects = query.order_by(asc(models.Object.name)).limit(limit + 1).all()
    
    is_truncated = len(objects) > limit
    next_marker = None
    
    if is_truncated:
        # The last object is the marker for the next page
        next_marker = objects[limit - 1].name
        # Return only the requested number of objects
        objects = objects[:limit]
        
    return objects, is_truncated, next_marker
def create_bucket(db: Session, name: str, owner_id: int):
    db_bucket = models.Bucket(name=name, owner_id=owner_id)
    db.add(db_bucket)
    db.commit()
    db.refresh(db_bucket)
    return db_bucket

def create_object(db: Session, bucket_id: int, name: str, size: int, etag: str, filepath: str, content_type: str):
    db_object = models.Object(
        bucket_id=bucket_id,
        name=name,
        size=size,
        etag=etag,
        filepath=filepath,
        content_type=content_type
    )
    db.add(db_object)
    db.commit()
    db.refresh(db_object)
    return db_object

def create_multipart_upload(db: Session, upload_id: str, bucket_name: str, object_name: str):
    upload = models.MultipartUpload(id=upload_id, bucket_name=bucket_name, object_name=object_name)
    db.add(upload)
    db.commit()
    db.refresh(upload)
    return upload

def get_multipart_upload(db: Session, upload_id: str):
    return db.query(models.MultipartUpload).filter(models.MultipartUpload.id == upload_id).first()

def create_multipart_part(db: Session, upload_id: str, part_number: int, etag: str, filepath: str):
    part = models.MultipartPart(upload_id=upload_id, part_number=part_number, etag=etag, filepath=filepath)
    db.add(part)
    db.commit()
    db.refresh(part)
    return part

def delete_multipart_upload(db: Session, upload_id: str):
    upload = get_multipart_upload(db, upload_id)
    if upload:
        db.delete(upload)
        db.commit()
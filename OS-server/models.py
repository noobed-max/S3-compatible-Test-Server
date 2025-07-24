from sqlalchemy import Boolean, Column, Integer, String, ForeignKey, DateTime
from sqlalchemy.orm import relationship
from datetime import datetime
from database import Base

class User(Base):
    __tablename__ = "users"
    id = Column(Integer, primary_key=True, index=True)
    access_key = Column(String, unique=True, index=True, nullable=False)
    secret_key = Column(String, nullable=False)
    buckets = relationship("Bucket", back_populates="owner")

class Bucket(Base):
    __tablename__ = "buckets"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, unique=True, index=True, nullable=False)
    owner_id = Column(Integer, ForeignKey("users.id"))
    owner = relationship("User", back_populates="buckets")
    objects = relationship("Object", back_populates="bucket")

class Object(Base):
    __tablename__ = "objects"
    id = Column(Integer, primary_key=True, index=True)
    name = Column(String, index=True, nullable=False)
    bucket_id = Column(Integer, ForeignKey("buckets.id"))
    size = Column(Integer, nullable=False)
    etag = Column(String, nullable=False)
    filepath = Column(String, nullable=False)
    content_type = Column(String, default="application/octet-stream")
    last_modified = Column(DateTime, default=datetime.utcnow)
    bucket = relationship("Bucket", back_populates="objects")

class MultipartUpload(Base):
    __tablename__ = "multipart_uploads"
    id = Column(String, primary_key=True, index=True)  # This is the upload_id
    bucket_name = Column(String, nullable=False)
    object_name = Column(String, nullable=False)
    created_at = Column(DateTime, default=datetime.utcnow)
    parts = relationship("MultipartPart", back_populates="upload", cascade="all, delete-orphan")

class MultipartPart(Base):
    __tablename__ = "multipart_parts"
    id = Column(Integer, primary_key=True, index=True)
    upload_id = Column(String, ForeignKey("multipart_uploads.id"))
    part_number = Column(Integer, nullable=False)
    etag = Column(String, nullable=False)
    filepath = Column(String, nullable=False)
    upload = relationship("MultipartUpload", back_populates="parts")
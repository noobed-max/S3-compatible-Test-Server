from xml.etree.ElementTree import Element, SubElement, tostring
import models

def generate_error_response(code: str, message: str, resource: str) -> bytes:
    root = Element("Error")
    SubElement(root, "Code").text = code
    SubElement(root, "Message").text = message
    SubElement(root, "Resource").text = resource
    return tostring(root, encoding="utf-8")

def initiate_multipart_upload_response(bucket: str, key: str, upload_id: str) -> bytes:
    root = Element("InitiateMultipartUploadResult", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
    SubElement(root, "Bucket").text = bucket
    SubElement(root, "Key").text = key
    SubElement(root, "UploadId").text = upload_id
    return tostring(root, encoding="utf-8")

def complete_multipart_upload_response(bucket: str, key: str, etag: str, location: str) -> bytes:
    root = Element("CompleteMultipartUploadResult", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
    SubElement(root, "Location").text = location
    SubElement(root, "Bucket").text = bucket
    SubElement(root, "Key").text = key
    SubElement(root, "ETag").text = etag
    return tostring(root, encoding="utf-8")


def generate_location_response() -> bytes:
    """Generates an S3-compatible LocationConstraint XML response."""
    root = Element("LocationConstraint", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
    return tostring(root, encoding="utf-8")

def generate_list_objects_v2_response(
    bucket_name: str,
    prefix: str,
    marker: str,
    max_keys: int,
    is_truncated: bool,
    objects: list[models.Object],
    next_marker: str,
) -> bytes:
    """Generates an S3-compatible ListBucketResult (V2) XML response."""
    root = Element("ListBucketResult", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
    SubElement(root, "Name").text = bucket_name
    SubElement(root, "Prefix").text = prefix
    SubElement(root, "MaxKeys").text = str(max_keys)
    SubElement(root, "IsTruncated").text = "true" if is_truncated else "false"

    for obj in objects:
        contents = SubElement(root, "Contents")
        SubElement(contents, "Key").text = obj.name
        SubElement(contents, "LastModified").text = obj.last_modified.strftime("%Y-%m-%dT%H:%M:%S.%fZ")
        SubElement(contents, "ETag").text = f'"{obj.etag}"'
        SubElement(contents, "Size").text = str(obj.size)
        SubElement(contents, "StorageClass").text = "STANDARD"

    if is_truncated and next_marker:
        SubElement(root, "NextContinuationToken").text = next_marker

    if marker:
        SubElement(root, "ContinuationToken").text = marker

    return tostring(root, encoding="utf-8")
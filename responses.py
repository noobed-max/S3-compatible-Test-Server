from xml.etree.ElementTree import Element, SubElement, tostring

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

# Add this function to your responses.py file

def generate_location_response() -> bytes:
    """Generates an S3-compatible LocationConstraint XML response."""
    root = Element("LocationConstraint", {"xmlns": "http://s3.amazonaws.com/doc/2006-03-01/"})
    # An empty tag implies the default region 'us-east-1', which is perfect for this use case.
    return tostring(root, encoding="utf-8")
import os
import hashlib
from pathlib import Path
import shutil
STORAGE_ROOT = Path("s3_storage")
STORAGE_ROOT.mkdir(exist_ok=True)

def create_bucket_folder(bucket_name: str):
    (STORAGE_ROOT / bucket_name).mkdir(exist_ok=True)

def save_object(bucket_name: str, object_name: str, data: bytes) -> tuple[int, str]:
    obj_path = STORAGE_ROOT / bucket_name / object_name
    obj_path.parent.mkdir(parents=True, exist_ok=True)
    with open(obj_path, "wb") as f:
        f.write(data)
    
    size = len(data)
    etag = hashlib.md5(data).hexdigest()
    return size, etag

def save_part(upload_id: str, part_number: int, data: bytes) -> tuple[str, str]:
    part_dir = STORAGE_ROOT / ".tmp" / upload_id
    part_dir.mkdir(parents=True, exist_ok=True)
    filepath = part_dir / f"part.{part_number}"
    with open(filepath, "wb") as f:
        f.write(data)
    
    etag = hashlib.md5(data).hexdigest()
    return str(filepath), etag

def combine_parts(bucket_name: str, object_name: str, parts: list) -> tuple[int, str]:
    final_path = STORAGE_ROOT / bucket_name / object_name
    final_path.parent.mkdir(parents=True, exist_ok=True)
    
    total_size = 0
    md5s = []

    with open(final_path, "wb") as final_file:
        # Sort parts by part number before combining
        parts.sort(key=lambda p: p.part_number)
        for part in parts:
            with open(part.filepath, "rb") as part_file:
                data = part_file.read()
                final_file.write(data)
                total_size += len(data)
                md5s.append(hashlib.md5(data).digest())
            os.remove(part.filepath)
    
    # Calculate multipart ETag
    digests = b"".join(md5s)
    etag = f'"{hashlib.md5(digests).hexdigest()}-{len(parts)}"'

    # Cleanup
    tmp_dir = STORAGE_ROOT / ".tmp" / parts[0].upload_id
    if os.path.exists(tmp_dir) and not os.listdir(tmp_dir):
        os.rmdir(tmp_dir)
        
    return total_size, etag
def cleanup_parts(upload_id: str):
    """Deletes the temporary directory for a given multipart upload."""
    part_dir = STORAGE_ROOT / ".tmp" / upload_id
    if part_dir.exists():
        shutil.rmtree(part_dir)
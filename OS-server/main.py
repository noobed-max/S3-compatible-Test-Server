import os
from fastapi import FastAPI
from sqlalchemy.orm import Session
from dotenv import load_dotenv

import crud
import models
from database import SessionLocal, engine
from router import router

# Load environment variables from .env file
load_dotenv()

# Create DB tables
models.Base.metadata.create_all(bind=engine)

app = FastAPI()

# Include the main router
app.include_router(router)

@app.on_event("startup")
def startup_event():
    # Get credentials from environment variables
    default_access_key = os.getenv("MINIO_ACCESS_KEY")
    default_secret_key = os.getenv("MINIO_SECRET_KEY")

    if not default_access_key or not default_secret_key:
        print("\nERROR: MINIO_ACCESS_KEY and MINIO_SECRET_KEY must be set in the .env file.")
        return

    # Create a default user for testing if it doesn't exist
    db = SessionLocal()
    user = crud.get_user_by_access_key(db, default_access_key)
    if not user:
        default_user = models.User(
            access_key=default_access_key,
            secret_key=default_secret_key
        )
        db.add(default_user)
        db.commit()
        db.refresh(default_user)
    db.close()
    
    print("\nServer is ready.")
    print("Default credentials for Minio Client loaded from .env file:")
    print(f"  Access Key: {default_access_key}")
    print(f"  Secret Key: {default_secret_key}\n") # Mask the secret key for security

@app.get("/")
def read_root():
    return {"message": "MinIO Compatible FastAPI Server is running."}

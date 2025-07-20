from fastapi import FastAPI, Depends, HTTPException
from sqlalchemy.orm import Session

import crud
import models
from database import SessionLocal, engine, get_db
from router import router

# Create DB tables
models.Base.metadata.create_all(bind=engine)

app = FastAPI()

# Include the main router
app.include_router(router)

@app.on_event("startup")
def startup_event():
    # Create a default user for testing if it doesn't exist
    db = SessionLocal()
    user = crud.get_user_by_access_key(db, "minioadmin")
    if not user:
        default_user = models.User(
            access_key="minioadmin",
            secret_key="minioadmin"
        )
        db.add(default_user)
        db.commit()
        db.refresh(default_user)
    db.close()
    print("\nServer is ready.")
    print("Default credentials for Minio Client:")
    print("  Access Key: minioadmin")
    print("  Secret Key: minioadmin\n")

@app.get("/")
def read_root():
    return {"message": "MinIO Compatible FastAPI Server is running."}
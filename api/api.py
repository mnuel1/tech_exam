from fastapi import FastAPI, File, UploadFile, HTTPException
import os
from pymongo import MongoClient
from fastapi.responses import JSONResponse
from typing import Optional
from pymongo.server_api import ServerApi

from config.db import uri, DB_NAME, COLLECTION_NAME

app = FastAPI()

""" initiate database """
client = MongoClient(uri, server_api=ServerApi('1'))        
db = client[DB_NAME]
collection = db[COLLECTION_NAME]

@app.get("/")
def read_root():
    return {"message": "Hello, World"}

@app.post("/upload")
async def upload_csv(file: UploadFile = File(...)):

    """ 5MB max file size """
    max_file_size = 5 * 1024 * 1024

    """ Only accepts csv files"""
    if not file.filename.endswith('.csv'):
        raise HTTPException(status_code=400, detail="Invalid file type. Please upload a CSV file.")
    
    contents = await file.read()
    if len(contents) > max_file_size:
        raise HTTPException(status_code=400, detail="File is too large. Maximum size is 5MB.")

    upload_dir = "storage/app/medalists/"
    os.makedirs(upload_dir, exist_ok=True)

    file_path = os.path.join(upload_dir, file.filename)
    with open(file_path, "wb") as f:
        f.write(contents)

    return {"message": "CSV file uploaded succesfully.!"}

@app.get("/aggregated_stats/event")
async def aggregated_stats(page_number : int, page_size: Optional[int] = 10):
   
    skip = (page_number - 1) * page_size
    
    """ Get the total number of documents for pagination calculations """
    total_documents = collection.count_documents({})
    total_pages = (total_documents + page_size - 1) // page_size  
    
    """ Pagination metadata"""
    next_page = f"/aggregated_stats/event?page={page_number + 1}" if page_number < total_pages else None
    previous_page = f"/aggregated_stats/event?page={page_number - 1}" if page_number > 1 else None

    """ 
        Aggregation for the return json format, 
        I grouped all the medalist depending on the same discipline, event and event_date    

        to achieve the sample output 
        {
            "discipline": "Cycling Road",
            "event": "Men's Individual Time Trial",
            "event_date": "<medal date>",
            "medalists": [
                {
                    "name": "John Doe",
                    "medal_type": "Gold",
                    "gender": "Male",
                    "country": "USA",
                    "country_code": "US",
                    "nationality": "American",
                    "medal_code": "G",
                    "medal_date": "<medal date>"
                }
                 ...additional medalists
            ]
        }
    """
    try:
        aggregation_pipeline = [
            {"$skip": skip},  # Skip the documents based on pagination
            {"$limit": page_size},  # Limit the number of documents per page
            {
                "$group": {
                    "_id": {
                        "discipline": "$discipline",
                        "event": "$event",
                        "event_date": "$medal_date"
                    },
                    "medalists": {
                        "$push": {
                            "name": "$name",
                            "medal_type": "$medal_type",
                            "gender": "$gender",
                            "country": "$country",
                            "country_code": "$country_code",
                            "nationality": "$nationality",
                            "medal_code": "$medal_code",
                            "medal_date": "$medal_date"
                        }
                    }
                }
            },
            {
                "$project": {
                    "_id": 0,
                    "discipline": "$_id.discipline",
                    "event": "$_id.event",
                    "event_date": "$_id.event_date",
                    "medalists": 1
                }
            }
        ]
        
        """ Perform aggregation """
        result = list(collection.aggregate(aggregation_pipeline))
        
        if not result:
            raise HTTPException(status_code=404, detail="No data found.")
                
        response = {
            "data": result,
            "paginate": {
                "current_page": page_number,
                "total_pages": total_pages,
                "next_page": next_page,
                "previous_page": previous_page
            }
        }
        
        return JSONResponse(content=response)
    
    except Exception as e:
        raise HTTPException(status_code=500, detail=str(e))

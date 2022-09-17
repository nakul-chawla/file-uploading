import os
import shutil
import time
import json
import calendar
import datetime
import calendar
import PyPDF2
import logging

from pathlib import Path
from typing import Callable
from tempfile import NamedTemporaryFile

from fastapi import FastAPI, File, UploadFile, BackgroundTasks, HTTPException

from fastapi.routing import APIRoute
from pydantic import BaseModel
from fastapi.middleware import Middleware
from fastapi.middleware.cors import CORSMiddleware
from wand.image import Image
import subprocess

from alembic.runtime.migration import MigrationContext
from sqlalchemy import create_engine

from app.database import db_instance
from app.workers.celery import celery_app
from app.workers.tasks import run_test_task, process_files


# common functions
def check_valid_user(unique_id,user_id):
    """For now we have the user id in the unique_id, 
    the user who uploaded a file. 
    We verify by checking that the request to get path or status is by the same user or not. 
    """
    user = unique_id.split('_')[0]
    if user == user_id:
        return True
    else:
        return False


def check_all_pdf_paths(unique_id:str,filepath:str):
    """To find the paths of all converted files of pdf.
    We get the number of pages first, 
    which help us make the filepath of each image, we check all images exist or not to give the status, 
    if a page's image does'nt exist then we consider that the status is incomplete. 
    """
    file = open(filepath,'rb')
    flag = True
    readpdf = PyPDF2.PdfFileReader(file)
    pages = readpdf.numPages
    logging.info(pages)
    result = []
    for num in range(1,pages+1):
        final_file_path = "/scratch/"+unique_id + "_converted_"+str(num)+".png"
        if os.path.exists(final_file_path):
            print(final_file_path)
            result.append(final_file_path)
        else:
            flag=False
    if flag:
        return result,True           
    else:
        return result,False


def get_more_info(unique_id:str):
    """ to get the filetype and original file path
    """
    filetype = unique_id.split('_')[-1]
    
    original_file_path = "/scratch/"+unique_id + "_original." + filetype

    return filetype,original_file_path


# api routes

async def root():
    return {"Hello": "World"}


async def upload_file_task1(user_id:int, upload_files: list[UploadFile], bg_task:BackgroundTasks):
    """In this endpoint, 
    a file is uploaded and a background task is started to convert it into png.
    A unique id consisting of userid_unixtime_typeoffile is returned and used everywhere to access the files.
    This points at the first task. 
    """
    # we ahve to return the unique id's for all the files

    result = {
        "data":[]
    }
    # going to check if all the files are of a fine extension
    for upload_file in upload_files:
        file_ext = os.path.splitext(upload_file.filename)[1]

        if file_ext not in [".pdf",".jpeg",".jpg",".png"]:
            raise HTTPException(400, detail="Invalid document type of"+upload_file.filename)


    for count,upload_file in enumerate(upload_files):

        file_info = dict()
        file_ext = os.path.splitext(upload_file.filename)[1]
        current_time = datetime.datetime.utcnow()
        current_unix_time = calendar.timegm(current_time.utctimetuple())
        
        file_info["filename"] = upload_file.filename
        unique_id = str(user_id) + "_" + str(current_unix_time) + "_" +str(count)  + "_" +file_ext.split('.')[1]
        file_info["unique_id"] = unique_id  
        destination = Path("/scratch/"+unique_id+"_original"+ file_ext)
        file_info["original_file_path"] = destination  
        result["data"].append(file_info)
        
        try:
            with destination.open("wb") as buffer:
                shutil.copyfileobj(upload_file.file, buffer)
    
            bg_task.add_task(pre_process,unique_id,str(destination),upload_file.content_type)        
        except Exception as ex:
            raise HTTPException(500, detail="The upload/starting background task failed.")
        finally:
            upload_file.file.close()
    
    return result
    # return {"original_file_path": destination, "file_id": unique_id}


async def upload_file(user_id:int, upload_files: list[UploadFile]):
    """In this endpoint,
    the file is uploaded and a celery task is started to convert it into png.
    Currently only handling one upload at a time.
    For every request the user gets a unique id. 
    A unique id consisting of userid_unixtime_typeoffile is returned and used everywhere to access the files.
    """    
    # we ahve to return the unique id's for all the files

    result = {
        "data":[]
    }
    # going to check if all the files are of a fine extension
    for upload_file in upload_files:
        file_ext = os.path.splitext(upload_file.filename)[1]

        if file_ext not in [".pdf",".jpeg",".jpg",".png"]:
            raise HTTPException(400, detail="Invalid document type of"+upload_file.filename)


    for count,upload_file in enumerate(upload_files):

        file_info = dict()
        file_ext = os.path.splitext(upload_file.filename)[1]
        current_time = datetime.datetime.utcnow()
        current_unix_time = calendar.timegm(current_time.utctimetuple())
        
        file_info["filename"] = upload_file.filename
        unique_id = str(user_id) + "_" + str(current_unix_time) + "_" +str(count)  + "_" +file_ext.split('.')[1]
        file_info["unique_id"] = unique_id  
        destination = Path("/scratch/"+unique_id+"_original"+ file_ext)
        file_info["original_file_path"] = destination  
        result["data"].append(file_info)
        
        try:
            with destination.open("wb") as buffer:
                shutil.copyfileobj(upload_file.file, buffer)
    """Particular reason to use the apply async was to control the task, give eta, expires 
    """
            process_files.apply_async(args=[unique_id,str(destination),upload_file.content_type])
        except Exception as ex:
            raise HTTPException(500, detail="The upload/starting background task failed.")
        finally:
            upload_file.file.close()
    
    return result


def pre_process(unique_id,filepath,filetype):
    """This is a background task.
    Check the file type using the unique id, 
    For pdf/jpeg/jpg - convert the file using wand
    PDF - naming convention - uniqueid_converted_pagenumber.png
    jpeg/jpg/png - naming convention - uniqueid_converted.pnng
    """

    if filetype == "application/pdf":
        pdf = Image(filename=filepath)
        pdf_image = pdf.convert("png")
        count = 1
        for one_image in pdf_image.sequence:
            page = Image(image = one_image)
            page.save(filename="/scratch/"+unique_id+"_converted_"+str(count)+".png")
            count = count + 1
    else:
        img = Image(filename=filepath)
        if img.size[0]>3500 and img.size[1]>3500:
            img.resize(3500,3500)

        if filetype in ["image/jpeg","image/jpg"]:
            img = img.convert('png')
        
        img.save(filename="/scratch/"+unique_id+"_converted.png")
        

# Health checks
def get_alembic_version():
    db_url = db_instance.get_database_url()
    engine = create_engine(db_url)
    conn = engine.connect()
    context = MigrationContext.configure(conn)
    current_rev = context.get_current_revision()

    return current_rev


def celery_healthcheck():
    """Check if celery workers are alive with ping"""
    celery_response = celery_app.control.ping(timeout=0.5)
    if celery_response:
        return celery_response
    else:
        return "No celery tasks currently active."


async def celery_send_test_task():
    """Celery task test example
    Check worker_1 logs for info messages to see if task was successfully entered and exited."""
    run_test_task.delay()
    return "Check worker_1 logs."


async def healthcheck():
    """Basic healthcheck endpoint.
    Connects to DB for alembic version string and pings Celery worker(s) for 'pong' alive response.
    """
    alembic_revision = get_alembic_version()
    celery_response = celery_healthcheck()

    health_response = {
        "alembic_version": alembic_revision,
        "celery_response": celery_response,
    }

    return health_response



async def get_path(unique_id:str, user_id:str):
    """This function helps to get the path of the file for a unique id, 
    a unique id refers to one particular upload.
    Check authorized user or not, 
    check if pdf or not - if pdf check number of pages to get all file names
    otherwise use naming convention check if files exist and return.
    """

    if not check_valid_user(unique_id,user_id):
        return HTTPException(401, detail = "User is unauthorized to check files.")
    result = {
        "original_file_path":"",
        "final_file_paths":[]
    }
    # check whether the file was pdf or other
    filetype,original_file_path=get_more_info(unique_id)

    # check if the file exists
    if os.path.exists(original_file_path):
        result["original_file_path"] = original_file_path
    else:
        raise HTTPException(400, detail="Original document does not exist.")    
    
    if filetype == "pdf":
        pdf_paths,status = check_all_pdf_paths(unique_id,original_file_path)
        result["final_file_paths"] = pdf_paths    
    else:
        final_file_path = "/scratch/"+unique_id + "_converted.png"
        if os.path.exists(final_file_path):
            result["final_file_paths"].append(final_file_path)
    return result


def get_status(unique_id:str, user_id:str):
    """Check whether all the promised files exist or not and then
    accordingly return the status, if does'nt exist we assume the process is going on.
    did not consider the failure of the celery task.
    """

    if not check_valid_user(unique_id,user_id):
        return HTTPException(401, detail = "User is unauthorized to check files.")
    result = {
        "status":"in process"
    }
    filetype,original_file_path=get_more_info(unique_id)

    if os.path.exists(original_file_path):
        pass
    else:
        raise HTTPException(400, detail="Original document does not exist.")    

    if filetype == "pdf":
        pdf_paths,status = check_all_pdf_paths(unique_id,original_file_path)
        if status:
            result["status"] = "success"
    else:
        final_file_path = "/scratch/"+unique_id + "_converted.png"
        if os.path.exists(final_file_path):
            result["status"] = "success"
    return result
    

def get_resolution():
    """The filename and resolution of all images in the directory. 
    """
    result= {
        "output":[]
    }
    directory = "/scratch"
    for filename in os.listdir(directory):
        f = os.path.join(directory, filename)
        if os.path.isfile(f):
            img = Image(filename=f)
            temp = {
                "filename":"",
                "resolution":()
            }
            temp["filename"] = filename
            temp["resolution"] = img.size
            result["output"].append(temp)
    return result



routes = [
    APIRoute("/", endpoint=root, methods=["GET"]),
    APIRoute("/health", endpoint=healthcheck, methods=["GET"]),
    APIRoute("/test-task", endpoint=celery_send_test_task, methods=["GET"]),
    APIRoute("/upload_task1", endpoint=upload_file_task1, methods=["POST"]),
    APIRoute("/upload", endpoint=upload_file, methods=["POST"]),
    APIRoute("/paths", endpoint=get_path, methods=["GET"]),
    APIRoute("/status", endpoint=get_status, methods=["GET"]),
    APIRoute("/resolution", endpoint=get_resolution, methods=["GET"])
]

middleware = Middleware(CORSMiddleware)

app = FastAPI(routes=routes, middleware=[middleware])

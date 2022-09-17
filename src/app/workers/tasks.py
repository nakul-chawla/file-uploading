import logging
import asyncio
import time
from wand.image import Image

from sqlalchemy.orm import Session

from app.workers.celery import celery_app, BaseDbTask, loop


async def run_async_test_task(session: Session):
    # session is the db session from sqlalchemy
    logging.info("Entering test task (next message will appear in 5 seconds)")
    await asyncio.sleep(5)
    logging.info("Exiting test task")


@celery_app.task(
    bind=True,
    max_retries=3,
    acks_late=True,
    base=BaseDbTask,
    retry_jitter=True,
    retry_backoff=True,
    default_retry_delay=5,
    reject_on_worker_lost=True,
)
def run_test_task(self):
    try:
        # Example of async task running within celery
        loop.run_until_complete(run_async_test_task(self.session))
    except Exception as exc:
        logging.exception("exception while running task. retrying")
        raise self.retry(exc=exc)


@celery_app.task(
    bind=True,
    max_retries=3,
    acks_late=True,
    base=BaseDbTask,
    retry_jitter=True,
    retry_backoff=True,
    default_retry_delay=10,
    reject_on_worker_lost=True,
)
def process_files(self, unique_id:str, filepath:str, filetype:str):
    """This is a celery task
    Check the file type using the unique id, 
    For pdf/jpeg/jpg - convert the file using wand
    PDF - naming convention - uniqueid_converted_pagenumber.png
    jpeg/jpg/png - naming convention - uniqueid_converted.pnng
    """
    try:
        if filetype == "application/pdf":
            pdf = Image(filename=filepath)
            pdf_image = pdf.convert("png")
            count = 1
            for one_image in pdf_image.sequence:
                page = Image(image = one_image)
                page.save(filename="/scratch/"+unique_id+"_converted_"+str(count)+".png")
                count = count + 1
        else:
            img = Image(filename=".."+filepath)
            if img.size[0]>3500 and img.size[1]>3500:
                img.resize(3500,3500)

            if filetype in ["image/jpeg","image/jpg"]:
                img = img.convert('png')
            
            img.save(filename="/scratch/"+unique_id+"_converted.png")
        logging.info(unique_id + "completed task.")
    except Exception as exc:
        logging.exception("exception while running task. retrying")
        raise self.retry(exc=exc)
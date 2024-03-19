import json
import uvicorn
from fastapi import FastAPI, Body, HTTPException, status
import logging
from constent.constant import dynamodb_table_name, dynamodb
from utils.utils import put_item_in_dynamodb, process_video, process_image

logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI()


@app.post("/videos/process")
async def compress_video(bucket: str = Body(...), key: str = Body(...)):
    logger.info("compress video endpoint")

    table = dynamodb.Table(dynamodb_table_name)

    if not bucket or not key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=json.dumps({"message": "Missing required fields in request body: bucket and key"})
        )
    try:
        put_item_in_dynamodb(table, key, processed=0, error=None)

        process_video(bucket, key)

        put_item_in_dynamodb(table, key, processed=1, error=None)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "Video processed successfully"})
        }
    except Exception as e:
        logger.error(f"Error processing video: {e}")

        put_item_in_dynamodb(table, key, processed=0, error=str(e))

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=json.dumps({"message": "Failed to process video", "error": str(e)})
        )


@app.post("/image/process")
async def compress_image(bucket: str = Body(...), key: str = Body(...)):
    logger.info("compress image endpoint")
    table = dynamodb.Table(dynamodb_table_name)
    if not bucket or not key:
        raise HTTPException(
            status_code=status.HTTP_400_BAD_REQUEST,
            detail=json.dumps({"message": "Missing required fields in request body: bucket and key"})
        )
    try:
        put_item_in_dynamodb(table, key, processed=0, error=None)

        process_image(bucket, key)

        put_item_in_dynamodb(table, key, processed=1, error=None)

        return {
            "statusCode": 200,
            "body": json.dumps({"message": "image processed successfully"})
        }
    except Exception as e:
        logger.error(f"Error processing image: {e}")

        put_item_in_dynamodb(table, key, processed=0, error=str(e))

        raise HTTPException(
            status_code=status.HTTP_500_INTERNAL_SERVER_ERROR,
            detail=json.dumps({"message": "Failed to process image", "error": str(e)})
        )


if __name__ == "__main__":
    uvicorn.run(app, host="0.0.0.0", port=8000)

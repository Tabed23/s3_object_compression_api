import botocore.exceptions
from moviepy.editor import VideoFileClip
import logging
import io
from PIL import Image
from constent.constant import s3
import math
import subprocess

logger = logging.getLogger()
logger.setLevel("INFO")


def process_video(bucket, key):
    logger.info("process_video function")

    logger.info("Retrieving video from S3")
    filename = key.split('/')[1]
    try:
        logger.info("filename %s", filename)
        s3.download_file(Bucket=bucket, Key=key, Filename=filename)
        resized_clip = reduce_video_size(filename)
        resized_clip.write_videofile(filename)
        s3.upload_file(filename, bucket, key)
        subprocess.run(["rm", filename])
        logger.info("Video resized and uploaded back to S3")
    except Exception as e:
        logger.error("Error processing video: %s", e)


def reduce_video_size(video_input):
    logger.info("reducing the size of video from S3")
    clip = VideoFileClip(video_input)
    width_of_video = clip.w
    height_of_video = clip.h
    logger.info(f'Width and Height of original video: {width_of_video}x{height_of_video}')
    clip_resized = clip.resize(0.6)
    width_of_video = clip_resized.w
    height_of_video = clip_resized.h
    logger.info(f'Width and Height of original video: {width_of_video}x{height_of_video}')
    return clip_resized


def put_item_in_dynamodb(table, key, processed, error):
    logger.info("Updating the DynamoDB Table (put_item)")
    try:
        if error:
            item = {
                'object_key': key,
                'processed': processed,
                'error': error
            }
        else:
            item = {
                'object_key': key,
                'processed': processed,
            }

        table.put_item(Item=item)
        logger.info("Dynamo Table Updated")
    except botocore.exceptions.ClientError as e:
        logger.error("Error updating DynamoDB table: %s", e)


def get_file_size(bucket_name, object_key):
    response = s3.get_object(Bucket=bucket_name, Key=object_key)
    return response['ContentLength']


def process_image(bucket_name, object_key, max_size_mb=5):
    logger.info("process_image function")

    max_size_bytes = max_size_mb * 1024 * 1024

    try:
        file_size = get_file_size(bucket_name, object_key)

        if file_size <= max_size_bytes:
            logger.info(f"Image {object_key} is already within size limit ({file_size} bytes), no resizing needed.")
            return

        logger.info(f"Image {object_key} exceeds size limit ({file_size} bytes), resizing...")

        file_obj = s3.get_object(Bucket=bucket_name, Key=str(object_key))
        file_data = file_obj['Body'].read()
        with Image.open(io.BytesIO(file_data)) as img:
            if img.format in ['PNG', 'GIF']:
                if img.mode not in ['RGB', 'RGBA']:
                    logger.info("image mode is RGB or RGBA")
                    img = img.convert('RGBA')
            else:
                if img.mode in ['P', 'RGBA']:
                    logger.info("Image mode is P or RGBA")
                    img = img.convert('RGB')

            output_buffer = io.BytesIO()
            w_percent = math.sqrt(max_size_bytes / file_size)
            new_width = int(img.size[0] * w_percent)
            new_height = int(img.size[1] * w_percent)
            logger.info(new_height, new_width)

            resized_image = img.resize((new_width, new_height), Image.LANCZOS)
            logger.info("new Resize image")
            if img.format == 'PNG':
                resized_image.save(output_buffer, format='PNG', optimize=True)
                logger.info("PNG Save")
            elif img.format == 'GIF':
                resized_image.save(output_buffer, format='GIF', optimize=True)
                logger.info("GIF Save")
            else:
                resized_image.save(output_buffer, format='JPEG', optimize=True, quality=90)
                logger.info("JPEG")

            output_buffer.seek(0)

            s3.put_object(Bucket=bucket_name, Key=object_key, Body=output_buffer)
            subprocess.run(["rm", object_key])
            logger.info(f"Resized image uploaded to {object_key} in S3")

    except Exception as e:
        logger.error(f"Error processing {object_key}: {str(e)}")

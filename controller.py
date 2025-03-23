from dotenv import load_dotenv
load_dotenv('.env')

import asyncio
import boto3
import os
import time
import threading
from loguru import logger 


# ===
# AWS Config
# ===
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
INSTANCE_NAME = os.environ['PG_INSTANCE_NAME']
PG_HOST = os.environ['PG_HOST']
PG_PORT = os.environ['PG_PORT']
HOST = os.environ.get('CONTROLLER_HOST', 'localhost')
PORT = os.environ.get('CONTROLLER_PORT', '5432')
BUFFER_TIME_SECS = int(os.environ.get('TIMEOUT_SECS', 30 * 60))

session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY, region_name='us-east-1')
# ===
# Monitoring
# ===

async def forward_to_pg(reader, writer):
  pg_writer = None
  tasks = []
  try:
    pg_reader, pg_writer = await asyncio.open_connection(PG_HOST, PG_PORT)
    async def client_to_pg():
      try:
        while True:
          data = await reader.read(1024)
          if not data:
            break
          pg_writer.write(data)
          await pg_writer.drain()
      except asyncio.CancelledError:
        raise

    async def pg_to_client():
      try:
        while True:
          data = await pg_reader.read(1024)
          if not data: break
          writer.write(data)
          await writer.drain()
      except asyncio.CancelledError:
        raise


    # Store tasks so we can cancel them
    tasks = [
      asyncio.create_task(client_to_pg()),
      asyncio.create_task(pg_to_client())
    ]
    
    await asyncio.gather(*tasks)
  except Exception as e:
    logger.exception(f'Forwarding error {e}')
  finally:
    # Cancel any pending tasks
    for task in tasks:
      task.cancel()

    if pg_writer is not None:
      pg_writer.close()
      await pg_writer.wait_closed()
    writer.close()
    await writer.wait_closed()


def is_instance_running():
  ec2_client = session.client('ec2')
  
  # Find the instance by name tag
  response = ec2_client.describe_instances(
    Filters=[
      {'Name': 'tag:Name', 'Values': [INSTANCE_NAME]},
    ]
  )
  
  # Check if any instances are running
  for reservation in response['Reservations']:
    for instance in reservation['Instances']:
      if instance['State']['Name'] == 'running':
        return True
  
  return False


async def start_instance():
  ec2 = session.client('ec2')
  
  # Find instance ID by name tag
  response = ec2.describe_instances(
    Filters=[
      {'Name': 'tag:Name', 'Values': [INSTANCE_NAME]},
    ]
  )
  
  if not response['Reservations'] or not response['Reservations'][0]['Instances']:
    raise Exception(f"Instance with name {INSTANCE_NAME} not found")
    
  instance_id = response['Reservations'][0]['Instances'][0]['InstanceId']
  
  # Start the instance
  ec2.start_instances(InstanceIds=[instance_id])
  
  # Wait for the instance to be running
  waiter = ec2.get_waiter('instance_running')
  waiter.wait(InstanceIds=[instance_id])
  
  logger.info(f"Instance {INSTANCE_NAME} ({instance_id}) started successfully")


def get_instance_id():
  ec2 = session.client('ec2')
  
  response = ec2.describe_instances(
    Filters=[
      {'Name': 'tag:Name', 'Values': [INSTANCE_NAME]},
    ]
  )
  
  if not response['Reservations'] or not response['Reservations'][0]['Instances']:
    logger.error(f"Instance with name {INSTANCE_NAME} not found")
    return None
    
  return response['Reservations'][0]['Instances'][0]['InstanceId']

def stop_instance():
  time.sleep(BUFFER_TIME_SECS)
  logger.info(f"Stopping instance {INSTANCE_NAME} after {BUFFER_TIME_SECS} seconds of inactivity")
  
  instance_id = get_instance_id()
  if not instance_id:
    return
  
  try:
    ec2 = session.client('ec2')
    ec2.stop_instances(InstanceIds=[instance_id])
    
    logger.info(f"Instance {INSTANCE_NAME} ({instance_id}) stop request initiated")
    
    # Wait for the instance to stop
    waiter = ec2.get_waiter('instance_stopped')
    waiter.wait(InstanceIds=[instance_id])
    
    logger.info(f"Instance {INSTANCE_NAME} ({instance_id}) stopped successfully")
  except Exception as e:
    logger.exception(f"Error stopping instance: {e}")

# Track the stop instance thread
stop_thread = None

async def handle_connection(reader, writer):
  global stop_thread
  
  if is_instance_running():
    logger.debug('  instance is running, passing data')
    
    # Cancel existing stop thread if it's running
    if stop_thread and stop_thread.is_alive():
      logger.debug('  cancelling scheduled instance shutdown')
      stop_thread.cancel()
    
    await forward_to_pg(reader, writer)
    logger.debug('  data sent')

  else:
    logger.debug('  instance is not running, starting instance')
    await start_instance()
    writer.write(b'postgresql is starting, please retry in a moment\n')
    await writer.drain()
    writer.close()
    await writer.wait_closed()
    logger.debug('  message sent to sender')
  
  # Start a new thread to stop the instance after buffer time
  if not stop_thread or not stop_thread.is_alive():
    logger.debug('  scheduling instance shutdown')
    stop_thread = threading.Thread(target=stop_instance, daemon=True)
    stop_thread.start()
  

async def main():
  server = await asyncio.start_server(handle_connection, HOST, PORT)
  logger.info(f'Listening for PG requests on {HOST}:{PORT}')
  async with server:
    await server.serve_forever()


if __name__ == '__main__':
  asyncio.run(main())

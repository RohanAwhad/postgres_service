from dotenv import load_dotenv
load_dotenv('.env')

import boto3
import os
from loguru import logger 


# ===
# AWS Config
# ===
AWS_ACCESS_KEY_ID = os.environ['AWS_ACCESS_KEY_ID']
AWS_SECRET_KEY = os.environ['AWS_SECRET_ACCESS_KEY']
INSTANCE_NAME = os.environ['INSTANCE_NAME']
INSTANCE_IP = os.environ['INSTANCE_IP']

session = boto3.Session(aws_access_key_id=AWS_ACCESS_KEY_ID, aws_secret_access_key=AWS_SECRET_KEY, region_name='us-east-1')


# ===
# Monitoring
# ===

import asyncio

async def forward_to_pg(reader, writer, pg_ip):
  pg_writer = None
  tasks = []
  try:
    pg_reader, pg_writer = await asyncio.open_connection(pg_ip, 5434)
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


def get_instance_ip():
  return INSTANCE_IP


def is_instance_running():
  ec2 = session.resource('ec2')
  
  # Find the instance by name tag
  instances = list(ec2.instances.filter(
    Filters=[
      {'Name': 'tag:Name', 'Values': [INSTANCE_NAME]},
    ]
  ))
  
  if not instances:
    return False
    
  instance = instances[0]
  return instance.state['Name'] == 'running'

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


async def handle_connection(reader, writer):
  if is_instance_running():
    logger.debug('  instance is running, passing data')
    pg_ip = get_instance_ip()
    await forward_to_pg(reader, writer, pg_ip)
    logger.debug('  data sent')
    return

  logger.debug('  instance is not running, starting instance')
  await start_instance()
  writer.write(b'postgresql is starting, please retry in a moment\n')
  await writer.drain()
  writer.close()
  await writer.wait_closed()
  logger.debug('  message sent to sender')

async def main():
  server = await asyncio.start_server(handle_connection, '0.0.0.0', 5433)
  logger.info('Listening for PG requests on 5433')
  async with server:
    await server.serve_forever()

if __name__ == '__main__':
  asyncio.run(main())

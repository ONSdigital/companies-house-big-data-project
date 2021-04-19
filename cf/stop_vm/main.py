import base64
from googleapiclient import discovery
from oauth2client.client import GoogleCredentials

credentials = GoogleCredentials.get_application_default()
service = discovery.build('compute', 'v1', credentials=credentials)
print('VM Instance starting')

# Project ID for this request.
project = 'ons-companies-house-dev' 

# The name of the zone for this request.
zone = 'europe-west2-b'  

# Name of the instance resource to start.
instance = 'kirsty-vm-1'

def stop_vm_python(event, context):
    """Triggered from a message on a Cloud Pub/Sub topic.
    Args:
         event (dict): Event payload.
         context (google.cloud.functions.Context): Metadata for the event.
    """
    request = service.instances().stop(project=project, zone=zone, instance=instance)
    response = request.execute()

    print('VM Instance started')

    pubsub_message = base64.b64decode(event['data']).decode('utf-8')
    print(pubsub_message)

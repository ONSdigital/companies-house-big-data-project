from googleapiclient import discovery
from oauth2client.client import GoogleCredentials
import os
import subprocess

credentials = GoogleCredentials.get_application_default()
service = discovery.build('compute', 'v1', credentials=credentials)

print('VM Instance starting')

# Project ID for this request.
project = 'ons-companies-house-dev' 

# The name of the zone for this request.
zone = 'europe-west2-b'  

# Name of the instance resource to start.
instance = 'kirsty-vm-1'

def start_vm_python(event, context):

    request = service.instances().start(project=project, zone=zone, instance=instance)
    response = request.execute()

    print('VM Instance started')

    subprocess.call("ons-companies-house-dev/start_vm/start_vm.sh")

    #gcloud beta compute ssh --zone "europe-west2-b" "kirsty-vm-1" --tunnel-through-iap --project "ons-companies-house-dev"

    #os.system("python3 /repos/companies-house-big_data_project/cha_pipeline.py")
    
    
    print("end")
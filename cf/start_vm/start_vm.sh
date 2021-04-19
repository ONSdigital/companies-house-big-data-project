#!/bin/bash
import os

gcloud beta compute ssh --zone "europe-west2-b" "kirsty-vm-1" --tunnel-through-iap --project "ons-companies-house-dev"

os.system("python3 /repos/companies-house-big_data_project/cha_pipeline.py")

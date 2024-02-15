# FinOps Tool
This is a FinOps Tool that will give transparency into your AWS RDS and EC costs, as well as generating recommendations for cheaper resources
## Requirements
* EC2 Instance:
    * python installed
    * pip installed
    * boto3 installed
    * schedule library installed
    * prometheus-client library installed
* IAM Structure setup:
    *  finops tool role, in which context the EC2 instance will be running in and has permissions to assume finops member role
    *  finops member role,  that can be assumed by finops tool role and allows access to the services desired to monitor
## Getting Started
* clone the git repository into a suitable location on your EC2 instance
* cd into the directory where the `prometheus_exporter.py` is located
* run `python3 prometheus_exporter.py file_with_account_ids.csv`
* to run the tool and keep it running also after closing session to EC2 instance:
    * run `nohup python3 prometheus_exporter.py file_with_account_ids.csv > output.log 2>&1 &`
* now the cost metrics are being exposed on 'ec2-instance-ip':8000 and can be scraped by a prometheus client
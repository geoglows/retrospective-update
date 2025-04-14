# retrospective-update
Workflow to download ECMWF runoff data, route it using [river-route](https://river-route.hales.app/en/latest/), and append and upload to the GEOGLOWS AWS bucket. 

## Setup
### Create your EC2 Role
1. Go to the IAM service page and click on "Roles" in the navigation pane.
2. Click the "Create role" button.
3. Under "Trusted entity type" select "AWS Service". Under "Use case" select "EC2". Click "Next"
4. Search and select "AmazonS3FullAccess", "AmazonEC2FullAccess" and "CloudWatchLogsFullAccess". Click "Next"
5. Give your role a name. Click "Create role."

### Create your Lambda Role
1. Go to the IAM service page and click on "Roles" in the navigation pane.
2. Click the "Create role" button.
3. Under "Trusted entity type" select "AWS Service". Under "Use case" select "Lambda". Click "Next"
4. Search and select "AmazonS3FullAccess" and "AmazonEC2FullAccess". Click "Next"
5. Give your role a name. Click "Create role".

### Setup CloudWatch
1. Go to the AWS Cloudwatch service page and select "Log groups" in the navigation pane. Click "Create log group" in the top right corner.
2. Edit your group settings as you desire and click "Create".
3. With your new log group, click the "Create log stream" button. Give it a name. You will need to remember the group name and stream name when filling out the .profile file.

### Launch the computation instance
1. On the "Launch an instance" page, select a linux-based OS in "Application and OS Images (Amazon Machine Image)". 
2. Under "Instance Type", select your desired instance type. A possible choice is m5.2xlarge, which has eight vCPUs and 32 GiB of total memory. More CPUs generally equate to quicker execution due to the scripts' highly parallelized nature. Additionally, this instance type has better network performance.
3. Under "Key pair (login)", choose or create a key pair.
4. Under "Network settings" choose or create a security group. Enable "Auto-assign public IP".
5. Under "Configure Storage" choose an appropriate amount of storage to hold the OS and a few GBs of data.
6. Under "Advanced Details" select the EC2 IAM instance profile created earlier. 
7. Finally, click the "Launch instance" button.

### Setup the computation instance
1. Use SSH to connect to your instance (it should automatically start after launching). The command should look something like `ssh -i PATH/TO/KEY-PAIR.PEM ubuntu@IP-ADDRESS`. The IP address can be viewed on the Instances page of the EC2 service.
2. Run the following code in the EC2 instance's terminal:
``` 
cd $HOME
sudo apt-get update
sudo apt-get install git
git clone https://github.com/geoglows/retrospective-update.git
sudo chmod +x retrospective-update/install.sh
source retrospective-update/install.sh
```
3. Create a 1000GB volume (read how to do that [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-creating-volume.html)). Setup the instance to automatically attatch that volume on each startup (instructions under "Automatically mount an attached volume after reboot" [here](https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/ebs-using-volumes.html)). Remember the mount location you choose (we recommend simply using "/mnt"). Attach the volume.
4. Fill out the .profile file found in the retrospective-update folder
5. Stop the instance.

### Add user data
1. With the instance stopped, do the following:
    - Select the instance, click "Actions",  go to "Instance settings", and select "Edit user data".
    - Upload or copy-and-paste the coresponding *_user_data.txt from this repo to the Edit user data page. 
    - Save

    Everytime the instance starts up, the user data script will be run. If you need to access the instance, comment out the last two lines of code of the user data scripts that run Python and shut down the instance. 

### Create a lambda function
1. Go to the AWS Lambda service page. Click the "Create function" button.
2. Give your function a name. Select "Python 3.12" for the "Runtime" option. Under "Change default execution role"  select "Use an existing role" and choose the Lambda role you created previously. Click "Create function".
3. Replace the code provided with the following, inserting the appropriate values for region and instance IDs:
```
import json
import boto3

region = "INSERT_YOUR_REGION"

def lambda_handler(event, context):
    ec2 = boto3.client('ec2', region_name=region)
    ec2.start_instances(InstanceIds=["INSERT_EC2_INSTANCE_ID_HERE"])
```
   Make sure you click the "Deploy" button. Note that the region should not include letters after the number (i.e., us-west-2 instead of us-west-2a).
   4. In the configuration tab, set the timeout to be 0 min, 10 sec.

## Execution
You may test your lambda function to ensure that every step of this process succeeds. When you have fixed any potential errors and are ready to schedule this process, do the following:

1. Go to the Lamda function you created in the previous step. Click the "Add trigger" button.
2. Select "Eventbridge" from the dropdown. Select "Create a new rule". Select "Schedule expression". Enter a cron expression (for example, to set the lambda function to go off at 12:00 AM every Sunday, enter `cron(0 0 ? * SUN *)`).
3. Hit "Add"  and you're done!



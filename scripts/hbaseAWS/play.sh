#play
aws ec2 run-instances --image-id ami-XXXX --count 1 --instance-type t1.micro --key-name EC2AccessKeys --security-groups hbase
ssh -i EC2AccessKeys.pem -oStrictHostKeyChecking=no ec2-user@ec2-54-200-125-80.us-west-2.compute.amazonaws.com sudo sh play start

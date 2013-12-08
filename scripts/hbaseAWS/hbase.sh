#hbase
export EC2_HOME="../ec2-api-tools-1.6.12.0"
export JAVA_HOME="/root/Desktop/jre1.7.0_45"
export AWS_SECRET_KEY=sJKH3Zc3vClNLR5c7qzy7ANb7LSwB9w5geThi0Ox
export AWS_ACCESS_KEY=AKIAIZTF6CDD63GKX4GA  
./bin/ec2-run-instances ami-c2d349f2 -n 1 -t t1.micro 
#ssh -i EC2AccessKeys.pem -oStrictHostKeyChecking=no ec2-user@ec2-54-200-125-80.us-west-2.compute.amazonaws.com sudo sh ./hbase.sh
#./bin/ec2-describe-regions


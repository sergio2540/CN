#autoscalling
export EC2_HOME="../ec2-api-tools-1.6.12.0"
export AWS_CLOUDWATCH_HOME="../CloudWatch-1.0.13.4"
export AWS_AUTO_SCALING_HOME="../AutoScaling-1.0.61.3"
export PATH=$PATH:$AWS_AUTO_SCALING_HOME/bin:$EC2_HOME/bin:$AWS_CLOUDWATCH_HOME/bin
export JAVA_HOME="/root/Desktop/jdk1.7.0_45"
#export AWS_SECRET_KEY=sJKH3Zc3vClNLR5c7qzy7ANb7LSwB9w5geThi0Ox
#export AWS_ACCESS_KEY_ID=AKIAIZTF6CDD63GKX4GA
export AWS_AUTO_SCALING_URL=https://autoscaling.eu-west-1.amazonaws.com
export AWS_CREDENTIAL_FILE=./credentials
export EC2_REGION="eu-west-1"
#autoscalling configuration
as-delete-auto-scaling-group PlayGroup --force-delete
as-delete-launch-config --region eu-west-1 --launch-config playConfig
as-create-launch-config playConfig --image-id ami-d4d539a3 --instance-type t1.micro --group "Play" --user-data-file ./start_play.sh #--aws-credential-file ./credentials
as-delete-auto-scaling-group PlayGroup
as-create-auto-scaling-group PlayGroup --launch-configuration playConfig  --availability-zones eu-west-1a eu-west-1b --min-size 2 --max-size 4 --desired-capacity 2 --load-balancers PlayLoadBalancer
#ec2-describe-availability-zones --region eu-west-1
up=$(as-put-scaling-policy UpScalingPolicy --auto-scaling-group PlayGroup --adjustment=1 --type ChangeInCapacity) 
#--cooldown  ?? ##ver metricas aumenta em 30.cooldown de 300 segundos por defeito
down=$(as-put-scaling-policy DownScalingPolicy --auto-scaling-group PlayGroup --adjustment=-1 --type ChangeInCapacity) 
#--cooldown? ##ver metricas
#métrica para associar a alarme de cloudwatch. A métrica é a de uso de cpu, que tem em conta uso médio e que se prolonga por 120 segundos. O alarme dispara uso acima de 80%
mon-put-metric-alarm --alarm-name=AddCapacity --metric-name CPUUtilization --namespace "AWS/EC2" --statistic Average --period 60 --threshold 80 --comparison-operator GreaterThanOrEqualToThreshold --dimensions "ImageId=ami-d4d539a3" --evaluation-periods 3 --alarm-actions $up 
mon-put-metric-alarm --alarm-name=DeleteCapacity --metric-name CPUUtilization --namespace "AWS/EC2" --statistic Average --period 60 --threshold 50 --comparison-operator LessThanOrEqualToThreshold --dimensions "ImageId=ami-d4d539a3" --evaluation-periods 3 --alarm-actions $down
#mon-enable-alarm-actions AddCapacity DeleteCapacity
echo $up
echo $down

 #GreaterThanOrEqualToThreshold --dimensions "AutoScallingGroupName=myGroup" --evaluation-periods 2 --alarm-actions 

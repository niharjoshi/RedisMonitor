# Log File Monitoring & Alert System

---

## Introduction

In this project we build a log file monitoring system that sends email alerts to the project stakeholders when any **WARN** or **ERROR** logs are produced by our Log File Generator application.

Our entire project code base is written entirely in Scala and the pipeline is created using the following technology stack:

![Alt text](doc/technologies.jpg?raw=true "Technology Stack")

In the later sections, we will take a detailed look at our code + cloud architecture for this project.

The project comprises 3 Git repositories, each containing their own detailed README files with explanations:

- Project Component 1: https://github.com/niharjoshi/LogFileGeneratorDeployment.git
- **Project Component 2: https://github.com/niharjoshi/RedisMonitor.git (current)**
- Project Component 3: https://github.com/niharjoshi/SparkLogAlertSystem.git

---

## Prerequisites, Installation & Deployment

**A YouTube playlist documenting the deployment process can be found here: https://www.youtube.com/playlist?list=PL0k75q4RIbeuLDnClDzYQ0gZvzSnBlZ8Q**

We recommend cloning this repository onto your local machine and running it from the command-line using the interactive build tool **sbt**.

*Note: In order to install sbt, please follow the OS-specific instructions at https://www.scala-sbt.org/1.x/docs/Setup.html.*

To clone the repo use:
```console
git clone https://github.com/niharjoshi/RedisMonitor.git
```

Navigate to the repo and use the following command to run the unit test cases:
```console
sbt clean test
```

Next, sbt downloads project dependencies and compiles our Scala classes.
To do this, use the following command:
```console
sbt clean compile
```

To run the Redis monitor locally, you will need to start a Redis server on as well as a Kafka broker on localhost.

*Note: In order to install Redis and redis-cli, please follow the instructions at https://redis.io/topics/quickstart*

*Note: In order to install Kafka, please follow the instructions at https://kafka.apache.org/quickstart*

Next, change the **REDIS_HOST** parameter in the application configuration file at ```src/main/resources/application.conf``` to **localhost**. Change the **broker** parameter to **localhost:9092** and the **topic** parameter to your desired topic name.

To use a cloud-hosted Redis server or Kafka broker, follow the same instructions to change your Redis host and Kafka broker/topic, but do not forget to add your **base64 encoded AWS_ACCESS_KEY and AWS_SECRET_KEY** to the application configuration.

Lastly, run the application using:
```console
sbt run
```

### **If you want to run the application over an AWS EKS cluster, follow the steps below:**

First, create a AWS ECR repository to host the application's Docker image (https://docs.aws.amazon.com/AmazonECR/latest/userguide/repository-create.html). Name the repository **redis_monitor**.

From your terminal, log into using AWS ECR using the following command (replace your account ID and AWS region name accordingly):
```console
aws ecr get-login-password --region your_aws_region | docker login --username AWS --password-stdin your_account_id.dkr.ecr.your_aws_region.amazonaws.com

Example
aws ecr get-login-password --region us-east-2 | docker login --username AWS --password-stdin 824124XXXXXX.dkr.ecr.us-east-2.amazonaws.com
```

Next, from the root of the repository, build the Docker image:
```console
docker build -t redis_monitor -f docker/Dockerfile . --platform linux/amd64
```

Once the build is complete, tag the image with the latest tag and the URL of your ECR repository:
```console
docker tag redis_monitor:latest your_account_id.dkr.ecr.your_aws_region.amazonaws.com/redis_monitor:latest

Example
docker tag redis_monitor:latest 824124XXXXXX.dkr.ecr.us-east-2.amazonaws.com/redis_monitor:latest
```

Lastly, push the image to the ECR repo:
```console
docker push your_account_id.dkr.ecr.your_aws_region.amazonaws.com/redis_monitor:latest

Example
docker push 824124XXXXXX.dkr.ecr.us-east-2.amazonaws.com/redis_monitor:latest
```

*Note: This doumentation assumes that you have kubectl and eksctl set up along with your AWS command line interface. If not, please follow:*
- *kubectl: https://kubernetes.io/docs/tasks/tools/*
- *awscli: https://docs.aws.amazon.com/cli/latest/userguide/cli-chap-configure.html*
- *awscli IAM auth: https://docs.aws.amazon.com/eks/latest/userguide/install-aws-iam-authenticator.html*
- *eksctl: https://github.com/weaveworks/eksctl*

The Kubernetes config for this project is created using a network configuration of 1 VPC and 3 public subnets. You will have to create your own VPC and subnets. Once done, replace the identifiers for networking components in the cluster YAML at ```kubernetes/cluster.yaml```. Example:
```console
apiVersion: eksctl.io/v1alpha5
kind: ClusterConfig
metadata:
  name: LogFileGeneratorKubeCluster
  region: us-east-2

vpc:
  id: vpc-b55932de
  cidr: "172.31.0.0/16"
  subnets:
    public:
      us-east-2a:
        id: subnet-5155c63a
      us-east-2b:
        id: subnet-6438f119
      us-east-2c:
        id: subnet-b56c5cf9

nodeGroups:
  - name: EKS-public-workers
    instanceType: t2.medium
    desiredCapacity: 2

```

To create the cluster on AWS, use:
```console
eksctl create cluster -f kubernetes/cluster.yaml
```

This process takes about 20 minutes. Once done, verify is the cluster is created using:
```console
kubectl get svc
```

Next, to deploy the Redis monitor application to the newly created cluster, edit the ```kubernetes/deployment.yaml``` file and replace the **image** tag with the URI of your ECR Docker image. Example:
```console
apiVersion: apps/v1
kind: Deployment
metadata:
  name: log-file-generator-deployment
  namespace: default
spec:
  replicas: 1
  selector:
    matchLabels:
      app: web
  template:
    metadata:
      labels:
        app: web
    spec:
      containers:
        - name: log-file-generator
          image: 824124XXXXXX.dkr.ecr.us-east-2.amazonaws.com/redis_monitor:latest

```

Before we deploy, please make the configuration changes mentioned in the cloud-hosted Redis and Kafka section above.

Lastly, to deploy the application onto your Kubernetes cluster, use:
```console
kubectl apply -f kubernetes/deployment.yaml
```

Verify if logs are being written to your Kafka topic. If yes, the deployment is complete.

To see all the produced logs, you can use the Kafka console consumer mentioned in the Apache Kafka quickstart (https://kafka.apache.org/quickstart):
```console
bin/kafka-console-consumer.sh --topic logs --from-beginning --bootstrap-server localhost:9092
```

Sample output:

![Alt text](doc/kafka-logs.png?raw=true "Kafka Logs")

---

## Architecture & Flow of Control

**A YouTube playlist documenting the detailed architecture and flow of control can be found here: https://www.youtube.com/playlist?list=PL0k75q4RIbeuLDnClDzYQ0gZvzSnBlZ8Q**

### Note: This repository pertains to **Project Component 2**.

![Alt text](doc/flowchart.jpg?raw=true "Flow of Control")

To explain in brief, the core Redis monitor application is deployed onto a AWS EKS Kubernetes cluster over a deployment of 2 pods (each running one Docker container of the app).

The logs generated by the application are written into an AWS ElastiCache Redis database using a predefined UUID-based key-value schema.

Next, our Akka actor system running on another Kubernetes deployment continuously monitors the Redis DB and looks for newly added logs.

It filters the WARN and ERROR logs our and puts them into a AWS MSK Apache Kafka topic which are then consumed by a AWS EMR Apache Spark cluster with 1 master and 2 slave nodes.

The Spark application batches the consumed logs and subsequently sends email alerts to the project stakeholders.

### **Project Component 2 Design**

When we run the Redis monitor, our first Akka actor periodically probes the Redis DB for logs with keys starting with **p-**. These keys refer to the keys that have not been processed yet. Notice how all the logs are of type **p-***.

![Alt text](doc/logs-in-redis.png?raw=true "Logs In Redis")

Next, it gets the values of these keys and checks for **WARN** or **ERROR** log levels in them. If found, these key-value pairs are passed on to the second Akka actor.

Before transferring the execution to the second Akka actor, the first Akka actor changes the keys for all processed logs from **p-** to **d-** indicating that the processing is complete.

![Alt text](doc/logs-in-redis-modified.png?raw=true "Modified Logs In Redis")

The monitor then invokes the second Akka actor which simply passes on the logs supplied by the first Akka actor to the Kafka topic. Notice how each log is either of type **WARN** or **ERROR**.

![Alt text](doc/kafka-logs.png?raw=true "Kafka Logs")

---

## Checklist

- [x] All tasks completed
- [x] Installation instructions in README
- [x] In-depth documentation
- [x] Successful AWS deployment
- [x] YouTube video
- [x] More than 5 unit tests
- [x] Comments and explanations
- [x] Logging statements
- [x] No hardcoded values
- [x] No var or heap-based variables used
- [x] No for, while or do-while loops used

#!/bin/bash
aws s3 cp target/dewev4-1.0-SNAPSHOT.jar s3://331982-iad/ --region us-east-1
echo https://s3.amazonaws.com/331982-iad/dewev4-1.0-SNAPSHOT.jar

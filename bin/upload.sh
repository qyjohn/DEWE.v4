#!/bin/bash
aws s3 cp target/dewev4-1.0-SNAPSHOT.jar s3://331982-syd/ --region ap-southeast-2
echo https://s3.amazonaws.com/331982-syd/dewev4-1.0-SNAPSHOT.jar

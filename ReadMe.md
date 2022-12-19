# Architecture

![Architecture](DMSReplaceSystem.png)

# Steps

## Step 1: Start capturing change streams

Create and deploy the change streams app watching all changes from the cluster and write them as documents to a separate cluster

## Step 2: Start moving these events to the S3 bucket

Use Scheduled Triggers

## Step 3: Capture a new backup snapshot

Use Atlas Admin API

## Step 4: Export Snapshot to S3

Use Atlas Admin API

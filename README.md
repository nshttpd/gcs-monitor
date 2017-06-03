### gcs-monitor

Just a little application that was created to test out some things with
[Prometheus](https://prometheus.io/) and [Uber Zap](https://github.com/uber-go/zap). 

This will listen to a Google Cloud Platform Pub/Sub Queue that has been configured
for notifications around a Google Cloud Storage bucket. Documentation about this
can be found [here](https://cloud.google.com/storage/docs/pubsub-notifications).

There is an assumption here that you already have a GCP account, project, buckets
and know how it all works to add the Pub/Sub notifications onto a bucket.
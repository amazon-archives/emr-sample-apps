About EMRFSRSAEncryptionMaterialsProvider:
==========================================
EMRFSRSAEncryptionMaterialsProvider is an implementation of EncryptionMaterialsProvider that can be used as a plugin to EMRFS to support S3 client-side encryption using RSA key pair stored in S3, local file system in the cluster, or HDFS.


Generating RSA Key Pair:
========================
```
# Generate a 2048-bit RSA private key
$ openssl genrsa -out private_key.pem 2048

# Convert private key to PKCS#8 format with DER encoding
$ openssl pkcs8 -topk8 -inform PEM -outform DER -in private_key.pem -out private_key.der -nocrypt

# Generate public key portion with DER encoding
$ openssl rsa -in private_key.pem -pubout -outform DER -out public_key.der
```


Packaging:
==========
```
$ mvn package
```


Deploying:
==========
1. Deploy the RSA key pair:
Upload private_key.der and public_key.der to S3. Be sure to protect the key pair by setting up bucket policy appropriately.

2. Deploy the provider:
Upload EMRFSRSAEncryptionMaterialsProvider-1.0.jar to S3 and use the s3get bootstrap action to download the jar to all cluster nodes while launching cluster:
```
--bootstrap-actions Path=file:/usr/share/aws/emr/scripts/s3get,Args=[-s,<s3://path/to/EMRFSRSAEncryptionMaterialsProvider-1.0.jar>,-d,/usr/share/aws/emr/auxlib/]
```

3. Configure the provider:
```
--bootstrap-actions Path=file:/usr/share/aws/emr/scripts/configure-hadoop,Args=[-e,fs.s3.cse.enabled=true,-e,fs.s3.cse.encryptionMaterialsProvider=com.amazon.ws.emr.hadoop.fs.cse.RSAEncryptionMaterialsProvider,-e,fs.s3.cse.rsa.public=<s3://path/to/public_key.der>,-e,fs.s3.cse.rsa.private=<s3://path/to/private_key.der>,-e,fs.s3.cse.rsa.name=<NameOfKeyPair>]
```


Sample Command:
===============
```
$ aws emr create-cluster --ami-version=3.6.0 --instance-type m3.xlarge --instance-count 1 --ec2-attributes KeyName=<KeyName> --bootstrap-actions Path=file:/usr/share/aws/emr/scripts/s3get,Args=[-s,<s3://path/to/EMRFSRSAEncryptionMaterialsProvider-1.0.jar>,-d,/usr/share/aws/emr/auxlib/] Path=file:/usr/share/aws/emr/scripts/configure-hadoop,Args=[-e,fs.s3.cse.enabled=true,-e,fs.s3.cse.encryptionMaterialsProvider=com.amazon.ws.emr.hadoop.fs.cse.RSAEncryptionMaterialsProvider,-e,fs.s3.cse.rsa.public=<s3://path/to/public_key.der>,-e,fs.s3.cse.rsa.private=<s3://path/to/private_key.der>,-e,fs.s3.cse.rsa.name=<NameOfKeyPair>]
```

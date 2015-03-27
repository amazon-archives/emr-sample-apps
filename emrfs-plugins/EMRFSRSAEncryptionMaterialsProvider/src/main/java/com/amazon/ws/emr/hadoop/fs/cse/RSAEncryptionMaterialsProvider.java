package com.amazon.ws.emr.hadoop.fs.cse;

import com.amazonaws.auth.AWSCredentials;
import com.amazonaws.auth.AWSCredentialsProvider;
import com.amazonaws.auth.AWSCredentialsProviderChain;
import com.amazonaws.auth.BasicAWSCredentials;
import com.amazonaws.auth.InstanceProfileCredentialsProvider;
import com.amazonaws.regions.Regions;
import com.amazonaws.services.s3.AmazonS3;
import com.amazonaws.services.s3.AmazonS3Client;
import com.amazonaws.services.s3.model.EncryptionMaterials;
import com.amazonaws.services.s3.model.EncryptionMaterialsProvider;
import com.google.common.base.Preconditions;
import com.google.common.base.Strings;
import org.apache.commons.io.IOUtils;
import org.apache.hadoop.conf.Configurable;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;

import java.io.IOException;
import java.io.InputStream;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.KeyFactory;
import java.security.KeyPair;
import java.security.NoSuchAlgorithmException;
import java.security.PrivateKey;
import java.security.PublicKey;
import java.security.spec.InvalidKeySpecException;
import java.security.spec.PKCS8EncodedKeySpec;
import java.security.spec.X509EncodedKeySpec;
import java.util.Map;

/**
 * Provides encryption materials using rsa key pair stored in s3
 */
public class RSAEncryptionMaterialsProvider implements EncryptionMaterialsProvider, Configurable {
  private final String ACCESS_KEY_CONF = "fs.s3.awsAccessKeyId";
  private final String SECRET_KEY_CONF = "fs.s3.awsSecretAccessKey";
  private final String ENDPOINT_CONF = "fs.s3n.endpoint";
  private final String ENDPOINT_DEFAULT = "s3.amazonaws.com";

  private final String RSA = "RSA";
  private final String CSE_RSA_NAME = "rsa_name";
  private final String CSE_RSA_NAME_CONF = "fs.s3.cse.rsa.name";
  private final String CSE_RSA_PUBLIC_CONF = "fs.s3.cse.rsa.public";
  private final String CSE_RSA_PRIVATE_CONF = "fs.s3.cse.rsa.private";

  private AmazonS3 s3;
  private Configuration conf;
  private EncryptionMaterials encryptionMaterials;
  private String descValue;

  private void initializeAmazonS3() {
    if (s3 == null) {
      final String accessKey = conf.get(ACCESS_KEY_CONF);
      final String secretKey = conf.get(SECRET_KEY_CONF);
      s3 = new AmazonS3Client(new AWSCredentialsProviderChain(
          new AWSCredentialsProvider() {
            @Override
            public AWSCredentials getCredentials() {
              if (!Strings.isNullOrEmpty(accessKey) && !Strings.isNullOrEmpty(secretKey)) {
                return new BasicAWSCredentials(accessKey, secretKey);
              } else {
                return null;
              }
            }

            @Override
            public void refresh() {

            }
          },
          new InstanceProfileCredentialsProvider()));

      if (!Strings.isNullOrEmpty(conf.get(ENDPOINT_CONF))) {
        s3.setEndpoint(conf.get(ENDPOINT_CONF));
      } else if (Regions.getCurrentRegion() != null) {
        s3.setRegion(Regions.getCurrentRegion());
      } else {
        s3.setEndpoint(ENDPOINT_DEFAULT);
      }
    }
  }

  private void init() {
    try {
      this.descValue = this.conf.get(CSE_RSA_NAME_CONF);
      Preconditions.checkArgument(!Strings.isNullOrEmpty(descValue),
          String.format("%s cannot be empty", CSE_RSA_NAME_CONF));
      URI publicKeyURI = new URI(this.conf.get(CSE_RSA_PUBLIC_CONF));
      URI privateKeyURI = new URI(this.conf.get(CSE_RSA_PRIVATE_CONF));

      InputStream publicKeyIS, privateKeyIS;
      if ("s3".equalsIgnoreCase(publicKeyURI.getScheme()) || "s3n".equalsIgnoreCase(publicKeyURI.getScheme())) {
        initializeAmazonS3();
        String publicKeyS3Bucket = getBucket(publicKeyURI);
        String publicKeyS3Key = getKey(publicKeyURI);
        publicKeyIS = s3.getObject(publicKeyS3Bucket, publicKeyS3Key).getObjectContent();
      } else {
        Path publicKeyPath = new Path(publicKeyURI);
        FileSystem fs = publicKeyPath.getFileSystem(conf);
        publicKeyIS = fs.open(publicKeyPath);
      }

      if ("s3".equalsIgnoreCase(privateKeyURI.getScheme()) || "s3n".equalsIgnoreCase(privateKeyURI.getScheme())) {
        initializeAmazonS3();
        String privateKeyS3Bucket = getBucket(privateKeyURI);
        String privateKeyS3Key = getKey(privateKeyURI);
        privateKeyIS = s3.getObject(privateKeyS3Bucket, privateKeyS3Key).getObjectContent();
      } else {
        Path privateKeyPath = new Path(privateKeyURI);
        FileSystem fs = privateKeyPath.getFileSystem(conf);
        privateKeyIS = fs.open(privateKeyPath);
      }

      PublicKey publicKey = getRSAPublicKey(publicKeyIS);
      PrivateKey privateKey = getRSAPrivateKey(privateKeyIS);
      this.encryptionMaterials = new EncryptionMaterials(new KeyPair(publicKey, privateKey));
      this.encryptionMaterials.addDescription(CSE_RSA_NAME, descValue);
    } catch (URISyntaxException | IOException | NoSuchAlgorithmException | InvalidKeySpecException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public EncryptionMaterials getEncryptionMaterials(Map<String, String> materialsDescription) {
    if (materialsDescription == null
        || materialsDescription.get(CSE_RSA_NAME) == null
        || descValue.equals(materialsDescription.get(CSE_RSA_NAME))) {
      return this.encryptionMaterials;
    } else {
      throw new RuntimeException(
          String.format("RSA key pair (%s: %s) doesn't match with the materials description", CSE_RSA_NAME, descValue));
    }
  }

  @Override
  public EncryptionMaterials getEncryptionMaterials() {
    if (this.encryptionMaterials != null) {
      return this.encryptionMaterials;
    } else {
      throw new RuntimeException("RSA key pair is not initialized.");
    }
  }

  @Override
  public void refresh() {

  }

  @Override
  public Configuration getConf() {
    return this.conf;
  }

  @Override
  public void setConf(Configuration conf) {
    this.conf = conf;
    init();
  }

  private PrivateKey getRSAPrivateKey(InputStream privateKeyInputStream)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    byte[] privateKeyBytes = IOUtils.toByteArray(privateKeyInputStream);
    PKCS8EncodedKeySpec spec = new PKCS8EncodedKeySpec(privateKeyBytes);
    KeyFactory keyFactory = KeyFactory.getInstance(RSA);
    return keyFactory.generatePrivate(spec);
  }

  private PublicKey getRSAPublicKey(InputStream publicKeyInputStream)
      throws IOException, NoSuchAlgorithmException, InvalidKeySpecException {
    byte[] publicKeyBytes = IOUtils.toByteArray(publicKeyInputStream);
    X509EncodedKeySpec spec = new X509EncodedKeySpec(publicKeyBytes);
    KeyFactory keyFactory = KeyFactory.getInstance(RSA);
    return keyFactory.generatePublic(spec);
  }

  private String getBucket(URI s3Uri) throws URISyntaxException {
    return s3Uri.getHost();
  }

  private String getKey(URI s3Uri) throws URISyntaxException {
    return s3Uri.getPath().substring(1);
  }
}

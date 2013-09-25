#!/usr/bin/env ruby

require 'logger'
require 'time'
require 'cgi'
require 'uri'
require 'net/http'
require 'base64'
require 'openssl'
require 'rexml/document'
require 'rexml/xpath'

require 'aws_sdb'

domain = "FreeBase_Names_Guide"

# Amazon Elastic MapReduce will always have a /home/hadoop/conf/hadoop-site.xml file. One of
# of the configuration values in that file is your Amazon account key and secret.

file = File.read('/home/hadoop/conf/hadoop-site.xml')
xml = REXML::Document.new(file)

secret = xml.elements['/configuration/property[name="fs.s3n.awsSecretAccessKey"]'].elements['value'].text
key = xml.elements['/configuration/property[name="fs.s3n.awsAccessKeyId"]'].elements['value'].text

options = {}
options[:access_key_id] = key
options[:secret_access_key] = secret

sdb = AwsSdb::Service.new(options)

# create our domain; this is causes one more network hit than we need, but it saves us from making an extra installer; since we are batch processing it's OK
sdb.create_domain(domain)

STDIN.each_line do |line|

  # we are only going to have 2 words - the name and the id
  words = line.chomp.split(/\t/)

  # this will indicate whether we were successful writing to SimpleDB or not
  success = false

  # we will sleep and retry a few times before giving up
  decay = 0
  decay_max = 10

  if (words.size >= 2)

    id = words[0]
    count = words[1]

    while success == false

        # we may get a failure when trying to write to SimpleDB; we will use a simple back-off algorithm to try
        # again a few times

        begin

          # put this into SimpleDB, we don't care about dubes since we are using replaceable attributes
          attributes = {}
          attributes["Count"] = count
          attributes["Name"] = "empty"

          sdb.put_attributes(domain, id, attributes)

          # if we got here, we were successful so exit our loop
          success = true
        rescue

          # if there was an error, wait a little bit of time and then try again; eventually
          # we will just give up
          if (decay < decay_max)

            decay += 1
            sleep(decay)

          else

            # at this point just give up, something must just be wrong
            success = true

          end
        end
    end
  end
end

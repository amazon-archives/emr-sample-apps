# Copyright 2011-2013 Amazon.com, Inc. or its affiliates. All Rights Reserved.
#
# Licensed under the Apache License, Version 2.0 (the "License"). You
# may not use this file except in compliance with the License. A copy of
# the License is located at
#
#     http://aws.amazon.com/apache2.0/
#
# or in the "license" file accompanying this file. This file is
# distributed on an "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF
# ANY KIND, either express or implied. See the License for the specific
# language governing permissions and limitations under the License.


#!/bin/bash

if [ -z "$1" ] ; then
    echo "Error: expected a time argument in GMT timezone of the form '%Y-%m-%d %H:%M:%S'"
    exit -1
fi

while [[ "$(date -u +'%Y-%m-%d %H:%M:%S')" < "$1" ]] ; do
    echo "$(date -u +'%Y-%m-%d %H:%M:%S'): Time point $1 not reached yet, sleeping for 10 seconds"
    sleep 10
done

echo "$(date -u +'%Y-%m-%d %H:%M:%S'): Finished waiting"

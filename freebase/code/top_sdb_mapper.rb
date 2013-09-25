#!/usr/bin/env ruby

# This mapper will pull out names for us; it should be used with sdb_name_reducer.rb, which will store
# the names in SimpleDB for us.
# Arguably we just need the mapper in this case, but the thought is that using the reducer will help
# us scale out more in parallel

# read in each line from standard input
STDIN.each_line do |line|

  # split our line into words based on the tab
  words = line.chomp.split(/\t/)

  if (words.size >= 2)
    # we really only care if the 2nd word is an id
    id = words[0]
    count = words[1]

    puts "#{id}\t#{count}"
  end
end


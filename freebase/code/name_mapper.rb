#!/usr/bin/env ruby

# This mapper will pull out names for us; it should be used with name_reducer.rb, which will store
# the names in SimpleDB for us

# read in each line from standard input
STDIN.each_line do |line|

  # split our line into words based on the tab
  words = line.chomp.split(/\t/)

  if (words.size >= 2)
    # we really only care if the 2nd word is an id
    name = words[0]
    id = words[1]

    matches = id.scan("/guid/")

    if (matches.size > 0)
      puts "#{words[0]}\t#{words[1]}"
    end
  end
end


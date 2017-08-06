require 'yaml'

if ARGV.size < 1
  $stderr.puts "#{$0} docker-compose.yml"
  exit(1)
end

def sh(*cmd)
  puts("$ " + cmd.join(" "))
  system(*cmd)
end

compose = YAML.load_file(ARGV.first)
compose["services"].each do |_, service|
  new_name = "192.168.8.17:4000/#{service["image"]}"
  sh("docker", "tag", "#{service["image"]}", new_name)
  sh("docker", "push", new_name)
end

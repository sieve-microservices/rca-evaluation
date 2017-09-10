require 'yaml'

if ARGV.size < 1
  $stderr.puts "#{$0} docker-compose.yml"
  exit(1)
end

compose = YAML.load_file(ARGV.first)
compose["services"].delete_if {|_, srv| srv["labels"] && srv["labels"]["tk.higgsboson.no-scheduling"]}
compose["services"].each do |name, service|
  service.delete("build")
  service.delete("depends_on")
  if service["image"].start_with?("openstack-kolla")
    service["image"] = "docker-registry.openstack.sieve:4000/#{service["image"]}"
  end
end
YAML.dump(compose["services"], $stdout)

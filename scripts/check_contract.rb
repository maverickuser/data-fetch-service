# Verify the supplied OpenAPI representations remain semantically identical.
require 'json'
require 'yaml'
json = JSON.parse(File.read('docs/specs/data-processing-service-openapi.json'))
yaml = YAML.safe_load(File.read('docs/specs/data-processing-service-openapi.yaml'))
abort 'OpenAPI JSON/YAML differ' unless json == yaml
abort 'Unexpected API version' unless json.dig('info', 'version') == '1.0.0'
puts 'OpenAPI JSON/YAML parity passed.'

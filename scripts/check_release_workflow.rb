require 'open3'
require 'yaml'

root = File.expand_path('..', __dir__)
release = YAML.load_file(File.join(root, '.github/workflows/release.yml'))
integration = YAML.load_file(File.join(root, '.github/workflows/aws-integration.yml'))
destroy = YAML.load_file(File.join(root, '.github/workflows/destroy.yml'))
jobs = release.fetch('jobs')

def require_needs(jobs, job, dependencies)
  actual = Array(jobs.fetch(job).fetch('needs'))
  missing = dependencies - actual
  abort "#{job} must depend on #{missing.join(', ')}" unless missing.empty?
end

require_needs(jobs, 'preflight', ['gates'])
require_needs(jobs, 'package', ['preflight'])
require_needs(jobs, 'aws-integration', ['preflight'])
require_needs(jobs, 'deploy', ['package', 'aws-integration'])
abort 'release must not apply the shared network; fetch Lambdas run outside any VPC' if jobs.key?('network')
require_needs(jobs, 'apply', ['deploy'])
abort 'release must not run the smoke suite until it is redesigned' if jobs.key?('smoke')

apply = jobs.fetch('apply')
abort 'apply must run in the prod environment' unless apply['environment'] == 'prod'
apply_commands = Array(apply['steps']).map { |step| step['run'].to_s }.join("\n")
unless apply_commands.include?('reviewed-plan/infra/service/service.tfplan') && !apply_commands.include?('-auto-approve')
  abort 'apply must apply the reviewed saved plan, not a fresh one'
end
abort 'deploy must only plan the service stack' if Array(jobs.fetch('deploy')['steps']).any? { |step| step['run'].to_s.include?('terraform -chdir=infra/service apply') }

unless jobs.fetch('aws-integration').fetch('uses') == './.github/workflows/aws-integration.yml'
  abort 'release must call the disposable-resource AWS integration workflow'
end

triggers = integration['on'] || integration[true] || {}
unless triggers.key?('workflow_dispatch') && triggers.key?('workflow_call')
  abort 'AWS integration must support manual and reusable invocation'
end

destroy_triggers = destroy['on'] || destroy[true] || {}
abort 'destroy must be manual only' unless destroy_triggers.keys == ['workflow_dispatch']
abort 'destroy must share the release concurrency group' unless destroy.dig('concurrency', 'group') == release.dig('concurrency', 'group')
destroy_jobs = destroy.fetch('jobs')
require_needs(destroy_jobs, 'destroy', ['plan'])
abort 'destroy must require the typed confirmation' unless destroy_jobs.fetch('destroy')['if'].to_s.include?("inputs.confirm == 'data-fetch-service'")
abort 'destroy plan must run only from main' unless destroy_jobs.fetch('plan')['if'].to_s.include?("github.ref == 'refs/heads/main'")
destroy_jobs.each { |name, job| abort "destroy/#{name} must run in the prod environment" unless job['environment'] == 'prod' }
plan_commands = Array(destroy_jobs.fetch('plan')['steps']).map { |step| step['run'].to_s }.join("\n")
abort 'destroy plan must be a targeted destroy plan saved to a file' unless plan_commands.include?('plan -destroy') && plan_commands.include?('-target=') && plan_commands.include?('-out=destroy.tfplan')
abort 'destroy must never call terraform destroy or state rm' if destroy_jobs.values.flat_map { |job| Array(job['steps']) }.any? { |step| step['run'].to_s =~ /terraform\S* -chdir=\S+ (destroy|state rm)/ }
destroy_commands = Array(destroy_jobs.fetch('destroy')['steps']).map { |step| step['run'].to_s }.join("\n")
unless destroy_commands.include?('reviewed-plan/infra/service/destroy.tfplan') && !destroy_commands.include?('-auto-approve')
  abort 'destroy must apply the reviewed saved plan, not a fresh one'
end

# The kept set must be exactly the data resources in storage.tf.
keep = Regexp.new(destroy.fetch('env').fetch('KEEP_PATTERN'))
Dir[File.join(root, 'infra/service/*.tf')].each do |file|
  File.read(file).scan(/^resource "([^"]+)" "([^"]+)"/).each do |type, name|
    kept = keep.match?("#{type}.#{name}")
    storage = File.basename(file) == 'storage.tf'
    abort "#{type}.#{name} in #{File.basename(file)} would be kept by the teardown" if kept && !storage
    abort "#{type}.#{name} in storage.tf would be destroyed by the teardown" if storage && !kept
  end
end

[release, integration, destroy].each do |workflow|
  workflow.fetch('jobs').each do |job_name, job|
    Array(job['steps']).each do |step|
      next unless step['run']

      _, error, status = Open3.capture3('bash', '-n', stdin_data: step['run'])
      abort "#{job_name}/#{step['name'] || 'unnamed'} has invalid shell syntax: #{error}" unless status.success?
    end
  end
end

puts 'Release and destroy dependencies, kept resources, and shell syntax passed.'

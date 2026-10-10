require 'open3'
require 'yaml'

root = File.expand_path('..', __dir__)
release = YAML.load_file(File.join(root, '.github/workflows/release.yml'))
integration = YAML.load_file(File.join(root, '.github/workflows/aws-integration.yml'))
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
require_needs(jobs, 'smoke', ['apply'])

apply = jobs.fetch('apply')
abort 'apply must run in the approval-gated prod-apply environment' unless apply['environment'] == 'prod-apply'
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

[release, integration].each do |workflow|
  workflow.fetch('jobs').each do |job_name, job|
    Array(job['steps']).each do |step|
      next unless step['run']

      _, error, status = Open3.capture3('bash', '-n', stdin_data: step['run'])
      abort "#{job_name}/#{step['name'] || 'unnamed'} has invalid shell syntax: #{error}" unless status.success?
    end
  end
end

puts 'Release dependencies and shell syntax passed.'

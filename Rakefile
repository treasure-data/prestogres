require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake/extensiontask'

spec = eval File.read("prestogres.gemspec")
Rake::ExtensionTask.new('prestogres_config', spec) do |ext|
  ext.cross_compile = true
  ext.lib_dir = 'lib'
  ext.ext_dir = 'ext'
  #ext.cross_platform = 'i386-mswin32'
end

task :default => [:build]

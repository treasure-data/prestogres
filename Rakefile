require 'bundler'
Bundler::GemHelper.install_tasks

require 'rake/extensiontask'

spec = eval File.read("prestogres.gemspec")
Rake::ExtensionTask.new('prestogres', spec) do |ext|
  ext.cross_compile = true
  ext.lib_dir = File.join(*['lib', 'prestogres', ENV['FAT_DIR']].compact)
  #ext.cross_platform = 'i386-mswin32'
end

task :default => [:build]

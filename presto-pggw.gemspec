
Gem::Specification.new do |gem|
  gem.name          = "presto-pggw"
  gem.version       = File.read('VERSION').strip

  gem.authors       = ["Sadayuki Furuhashi"]
  gem.email         = ["frsyuki@gmail.com"]
  gem.description   = %q{Presto PostgreSQL protocol gateway}
  gem.summary       = %q{Presto PostgreSQL protocol gateway}
  gem.homepage      = "https://github.com/treasure-data/presto-pggw"
  gem.license       = "Apache 2.0"

  gem.files         = `git ls-files`.split($\)
  gem.executables   = gem.files.grep(%r{^bin/}).map{ |f| File.basename(f) }
  gem.test_files    = gem.files.grep(%r{^(test|spec|features)/})
  gem.require_paths = ["lib"]
  gem.has_rdoc = false

  gem.add_development_dependency 'bundler', ['~> 1.0']
  gem.add_development_dependency 'rake', ['~> 0.9.2']
  gem.add_development_dependency 'rake-compiler', ['~> 0.8.3']
end

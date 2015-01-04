require 'bundler'

begin
  Bundler.setup(:default, :test)
rescue Bundler::BundlerError => e
  $stderr.puts e.message
  $stderr.puts "Run `bundle install` to install missing gems"
  exit e.status_code
end

RSpec.configure do |config|
  #config.before(:each) { Timecop.freeze(Time.utc("2014")) }
  #config.after(:each) { Timecop.return }
end

require 'pg'

@connection = nil

def c
  @connection
end

def connect(options={})
  @connection = PG::Connection.open({
    host: '127.0.0.1',
    port: 5439,
    dbname: 'tpch',
    user: 'pg'
  }.merge(options))
end

def disconnect
  c.close if c
end

def exec(str)
  c.exec(str)
end

def exec_rows(str)
  rows = []
  exec(str).each_row {|row| rows << row }
  rows
end

def exec_first(str)
  rows = exec_rows(str)
  rows.size.should == 1
  rows.first
end

def presto_query_one
  exec_first("select count(1) - 4 as one from tiny.region")[0].to_i
end


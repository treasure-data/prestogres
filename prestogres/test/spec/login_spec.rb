
describe "login" do
  after(:each) do
    disconnect rescue nil
  end

  describe "connects" do
    it { connect }
    it("twice") { connect; disconnect; connect }
    it("twice with another user") { connect; disconnect; connect(user: 'msgpack') }
    it("to unknown catalog") { connect(dbname: 'nosush') }

    it "to the default catalog" do
      connect
      presto_query_one.should == 1
    end

    it "to a catalog" do
      connect(dbname: 'nosuch')
      lambda { presto_query_one }.should raise_error
    end
  end

  describe "hba overwrites" do
    it "presto_catalog" do
      connect(dbname: 'hive', user: "catalog_tpch")
      presto_query_one.should == 1
    end

    it "presto_schema" do
      connect(user: "schema_tiny")
      exec_first("select count(1) from region").should == ["5"]
    end
  end

  # TODO hba
end

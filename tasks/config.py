class Config:

    buckets = dict()
    task = None
    input = None
    only_delta = None

    def __init__(self, env, input, output, task, only_delta):

        self.input = input
        self.task = task
        self.only_delta = only_delta

        self.buckets.update({"input" : "s3://parsed-serps-" + env + "/" + input + "/S/"})

        if output is None:
            output = input

        self.buckets.update({"root" : env + "-databricks-"})
        self.buckets.update({"prefix_hbase" :  output + "/hbase"})
        self.buckets.update({"output_hbase" : "s3://" + env + "-databricks-test/"  + output + "/hbase/"})
        self.buckets.update({"delta" : "s3://" + env + "-databricks-test/" + output + "/delta/"})
        self.buckets.update({"delta_basic_type" : "s3://" + env + "-databricks-test/" + output + "/delta/serps_basic_type"})
        
        print("Setting task configs... ")
        for key, value in self.buckets.items():
            print(f"{key}: {value}")
    
    def get_bucket(self, key):
        
        return self.buckets[key]
import argparse

class Config:

    params = dict()
    args = None

    def __init__(self, args):
        
        print("args: " + str(args))

        if args.output == None:
            output = args.input
        else:
            output = args.output

        self.args = args
        self.params.update({"input" : "s3://" + args.env + "-dbtemplate123/"  + args.input + "/"})
        self.params.update({"output" : "s3://" + args.env + "-dbtemplate123/"  + output + "/"})
        self.params.update({"skip" : args.skip})
                
        print("Setting task configs... ")
        for key, value in self.params.items():
            print(f"{key}: {value}")
    
    def get_bucket(self, key):
        
        return self.params[key]
    
    def skip_task(self):
        
        return self.params['skip']

    def get_test_output(self):

        return (self.params)
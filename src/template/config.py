import argparse

class Config:

    params = dict()

    def __init__(self, args):
        
        print("args: " + str(args))

        self.params.update({"task" : args.task})
        self.params.update({"skip" : args.skip})
        self.params.update({"debug" : args.debug})
        self.params.update({"env" : args.env})
        self.params.update({"default_catalog" : args.default_catalog})
        self.params.update({"default_schema" : args.default_schema})

        print("Setting task configs... ")
        for key, value in self.params.items():
            print(f"{key}: {value}")

    def get_value(self, key):
        
        return self.params[key]
    
    def skip_task(self):
        
        return self.params['skip']

    def get_test_output(self):

        return (self.params)
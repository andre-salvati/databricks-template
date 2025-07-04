from .baseTask import BaseTask


class Setup(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Setup for integration tests ...")

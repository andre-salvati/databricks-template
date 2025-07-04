from .baseTask import BaseTask


class Validate(BaseTask):
    def __init__(self, config):
        super().__init__(config)

    def run(self):
        print("Validating integration tests ...")

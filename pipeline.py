from metaflow import step, FlowSpec
import os

class AirlinesDataPreparationFlow(FlowSpec):
    @step
    def start(self):
        print('Flow started');
        self.next(self.end)

    @step
    def end(self):
        print('Flow completed');

# Run the pipline by executing:
# python3 pipeline.py output-dot -Tpng -o graph.png
# to generate drawing of the pipeline
if __name__ == '__main__':
    AirlinesDataPreparationFlow()
    
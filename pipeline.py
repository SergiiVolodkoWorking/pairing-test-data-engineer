from metaflow import step, FlowSpec
import pandas as pd

from transformations import add_date,\
                            count_column_groups

class AirlinesDataPreparationFlow(FlowSpec):
    @step
    def start(self):
        print('Flow started')
        self.next(self.load_flights)

    @step
    def load_flights(self):
        self.flights_df = pd.read_csv('data/flights.csv')
        self.next(self.get_covered_days_count)

    @step
    def get_covered_days_count(self):
        df = add_date(self.flights_df)
        days_covered = count_column_groups(df, df['date'].dt.date)
        self.covered_days = days_covered
        self.next(self.end)

    @step
    def end(self):
        print('Flow completed')
        print(self.covered_days)

# Run the pipline by executing:
# python3 pipeline.py output-dot | dot -Tpng -o graph.png
# to generate drawing of the pipeline
if __name__ == '__main__':
    AirlinesDataPreparationFlow()
    
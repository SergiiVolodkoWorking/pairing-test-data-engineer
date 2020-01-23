from metaflow import step, FlowSpec
import pandas as pd

from transformations import add_date,\
                            count_column_groups,\
                            merge_detasets,\
                            extract_cities_from_airports,\
                            merge_flights_with_cities,\
                            add_connection_id,\
                            aggregate_connections_between_cities

class AirlinesDataPreparationFlow(FlowSpec):
    @step
    def start(self):
        print('Flow started')
        self.next(self.load_flights, self.load_planes, self.load_airports)

    @step
    def load_planes(self):
        self.planes_df = pd.read_csv('data/planes.csv')
        self.next(self.wait_all_data_is_loaded)

    @step
    def load_flights(self):
        self.flights_df = pd.read_csv('data/flights.csv')
        self.next(self.wait_all_data_is_loaded)
 
    @step
    def load_airports(self):
        self.airports_df = pd.read_csv('data/airports.csv')
        self.next(self.wait_all_data_is_loaded)

    @step
    def wait_all_data_is_loaded(self, inputs):
        # Metaflow special method to propagate values
        # that were calculated in parallel steps
        self.merge_artifacts(inputs)
        self.next(self.start_transformations)

    @step
    def start_transformations(self):
       self.next(
            self.get_covered_days_count,
            self.get_departure_cities_count,
            self.merge_flights_and_planes,
            self.calculate_cities_connectivity)

    @step
    def get_covered_days_count(self):
        df = add_date(self.flights_df)
        self.covered_days  = count_column_groups(df, df['date'].dt.date)
        self.next(self.wait_calculations_to_complete)

    @step
    def get_departure_cities_count(self):
        flight_origins_count = count_column_groups(self.flights_df, self.flights_df['origin'])
        self.departure_cities_count = flight_origins_count
        self.next(self.wait_calculations_to_complete)

    @step
    def merge_flights_and_planes(self):
        self.flights_and_planes_df = merge_detasets(self.planes_df, self.flights_df, 'tailnum')
        self.next(self.find_biggest_delays)

    @step
    def find_biggest_delays(self):
        flights_by_planes = self.flights_and_planes_df.groupby(['manufacturer'])
        self.max_departure_delays = flights_by_planes['dep_delay'].max()
        self.max_arrival_delays = flights_by_planes['arr_delay'].max()
        self.next(self.wait_calculations_to_complete)

    @step
    def calculate_cities_connectivity(self):
        cities = extract_cities_from_airports(self.airports_df)
        flights_with_cities = merge_flights_with_cities(self.flights_df, cities)
        flights_with_cities = add_connection_id(flights_with_cities)

        self.connections_between_cities = aggregate_connections_between_cities(flights_with_cities)

        self.next(self.wait_calculations_to_complete)

    @step
    def wait_calculations_to_complete(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        print('Flow completed')
        # print(self.covered_days)
        # print(self.departure_cities_count)

# Run the pipline by executing:
# python3 pipeline.py output-dot | dot -Tpng -o graph.png
# to generate drawing of the pipeline
# For vertical graph layout:
# python pipeline.py output-dot | dot -Grankdir=TB -Tpng -o graph.png
if __name__ == '__main__':
    AirlinesDataPreparationFlow()
    
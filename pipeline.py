from metaflow import step, FlowSpec
import pandas as pd
from os import path

from transformations import add_date,\
                            count_column_groups,\
                            merge_detasets,\
                            extract_cities_from_airports,\
                            merge_flights_with_cities,\
                            add_connection_id,\
                            aggregate_connections_between_cities,\
                            save_json_object_as_file

class AirlinesDataPreparationFlow(FlowSpec):
    data_folder = path.join(path.dirname(__file__), 'data')
    data_output_folder = path.join(path.dirname(__file__), 'data', 'output')

    @step
    def start(self):
        print('Flow started')
        self.next(self.load_flights, self.load_planes, self.load_airports)

    @step
    def load_planes(self):
        self.planes_df = pd.read_csv(path.join(self.data_folder, 'planes.csv'))
        self.next(self.wait_all_data_is_loaded)

    @step
    def load_flights(self):
        self.flights_df = pd.read_csv(path.join(self.data_folder, 'flights.csv'))
        self.next(self.wait_all_data_is_loaded)
 
    @step
    def load_airports(self):
        self.airports_df = pd.read_csv(path.join(self.data_folder, 'airports.csv'))
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
            self.find_biggest_delays,
            self.calculate_cities_connectivity)

    @step
    def get_covered_days_count(self):
        df = add_date(self.flights_df)
        self.covered_days = count_column_groups(df, df['date'].dt.date)
        self.next(self.wait_calculations_to_complete)

    @step
    def get_departure_cities_count(self):
        cities = extract_cities_from_airports(self.airports_df)
        flights_with_cities = merge_flights_with_cities(self.flights_df, cities)

        flight_origins_count = count_column_groups(flights_with_cities, flights_with_cities['origin_city'])
        self.departure_cities_count = flight_origins_count
        self.next(self.wait_calculations_to_complete)

    @step
    def find_biggest_delays(self):
        flights_and_planes_df = merge_detasets(self.planes_df, self.flights_df, 'tailnum')

        flights_by_planes = flights_and_planes_df.groupby('manufacturer')

        max_departure_delays = flights_by_planes['dep_delay'].max().reset_index()
        max_departure_delays = max_departure_delays.sort_values(by='dep_delay', ascending=False)
        self.max_departure_delays = max_departure_delays
        
        max_arrival_delays = flights_by_planes['arr_delay'].max().reset_index()
        max_arrival_delays = max_arrival_delays.sort_values(by='arr_delay', ascending=False)
        self.max_arrival_delays = max_arrival_delays

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
        self.merge_artifacts(inputs)
        self.next(self.start_saving_output_results)

    @step
    def start_saving_output_results(self):
        self.next(
            self.save_general_statistics,
            self.save_delays_data,
            self.save_cities_connectivity)

    @step
    def save_general_statistics(self):
        statistics = {
            'covered_days': self.covered_days,
            'departure_cities_count': self.departure_cities_count
            }

        file_path = path.join(self.data_output_folder, 'general_statistics.json')
        save_json_object_as_file(statistics, file_path)

        self.next(self.wait_all_results_saved)

    @step
    def save_delays_data(self):
        departure_delays_file = path.join(self.data_output_folder, 'departure_delays.csv')
        self.max_departure_delays.to_csv(departure_delays_file, index=False)

        arrival_delays_file = path.join(self.data_output_folder, 'arrival_delays.csv')
        self.max_arrival_delays.to_csv(arrival_delays_file, index=False)

        self.next(self.wait_all_results_saved)

    @step
    def save_cities_connectivity(self):
        cities_connectivity_file = path.join(self.data_output_folder, 'most_connected_cities.csv')

        self.connections_between_cities.to_csv(cities_connectivity_file, index=False)

        self.next(self.wait_all_results_saved)

    @step
    def wait_all_results_saved(self, inputs):
        self.next(self.end)

    @step
    def end(self):
        print('Flow completed.')

# Run the pipline by executing:
# python pipeline.py run
# to generate drawing of the pipeline
# For vertical graph layout:
# python pipeline.py output-dot | dot -Grankdir=TB -Tpng -o graph.png
if __name__ == '__main__':
    AirlinesDataPreparationFlow()
    
import random
from kafka import KafkaProducer
from api_handler import Api_Handler
from api_worker import Api_Worker

KAFKA_SERVER = 'localhost:9092'  # Docker container port exposed at 19092.
CLIENT_ID = 'real_api'

# Object used for sending temperature update messages to a Kafka topic
producer = KafkaProducer(bootstrap_servers=KAFKA_SERVER, client_id=CLIENT_ID)


def generate_id_list_randomly(num_cities=100, filename='all_city_ids.txt'):
    """!@brief Randomly select num_cities number of unique city ids from the given file, create and return a list with
    the unique city ids.

    @param num_cities    Number of unique city ids to randomly select
    @param filename        File from which to select random city ids

    @return a list of city ids equal in length to num_cities
    """

    antarctica_id = 6255152

    # Create a list from which random city ids should be sampled
    all_city_ids = generate_id_list_from_file(filename)

    # Randomly and uniquely select num_cities number within range 0 to the
    # total number of city ids (length of all_city_ids)
    indices = random.sample(range(0, len(all_city_ids)), num_cities - 1)

    # Use list of indices to generate a list of city ids
    random_city_ids = [all_city_ids[index] for index in indices]
    random_city_ids.append(antarctica_id)

    return random_city_ids


def get_id(coord_buckets, lat, lon):
    """
    Return the first city id in a coords bucket dictionary given latitude and
    longitude.

    @param coord_buckets    Dictionary of city ids broken up into buckets based
                            on latitude and longitude
    @param lat                lat bucket from which to retrieve a city id
    @param lon                lon bucket from which to retrieve a city id

    @return the first city id from the lat and lon bucket or None if one doesn't
    exist
    """
    return_id = None

    # If a bucket for lat exists...
    if coord_buckets.get(lat) is not None:
        id_list = coord_buckets[lat].get(lon)

        # If a bucket for lon exists...
        if id_list is not None:

            # Get the first city id from the bucket
            return_id = id_list.pop()

            # If the bucket is exhausted of city ids, delete it
            if len(id_list) == 0:
                coord_buckets[lat].pop(lon)
    return return_id


def generate_id_list_by_evenly_distributed_coordinates(num_ids=5000):
    # The real life range of values that a latitude may take on
    min_lat = -90
    max_lat = 90

    # The real life range of values that a longitude may take on
    min_lon = -180
    max_lon = 180

    # upper range on the number of latitude buckets (will always be 181)
    num_lat_buckets = (max_lat - min_lat) + 1

    # approximate number of ids that may be chosen from each latitude bucket
    ids_per_lat_bucket = int(num_ids / num_lat_buckets) + 1
    lon_increment = int(((max_lon - min_lon) + 1) / ids_per_lat_bucket)

    # List of ids being generated
    id_list = list()

    coord_buckets = create_coord_buckets_for_ids()

    # Evenly pick ids from each latitude bucket
    for curr_lat in range(min_lat, max_lat + 1):
        for curr_lon in range(min_lon, max_lon + 1, lon_increment):
            temp_lon = curr_lon
            city_id = None

            # Iteratively try longitudes from curr_lon to
            # curr_lon + lon_increment until an id is found
            while city_id is None and temp_lon < curr_lon + lon_increment:
                city_id = get_id(coord_buckets, curr_lat, temp_lon)
                temp_lon = temp_lon + 1

            # If there was a value at curr_lat, curr_lon in the dictionary
            if city_id:
                id_list.append(city_id)

    # Fill up the rest of the queue by going through each latitude bucket and
    # randomly choosing a longitude to pick from
    while len(id_list) < num_ids:
        curr_lat = min_lat

        while curr_lat < max_lat and len(id_list) < num_ids:
            curr_lon = random.randint(min_lon, max_lon)
            city_id = get_id(coord_buckets, curr_lat, curr_lon)
            if city_id:
                id_list.append(city_id)

            curr_lat = curr_lat + 1

    return id_list


def create_coord_buckets_for_ids(filename='ids_and_coords.txt'):
    """
    Given a file, with each line formatted as 'city id:city lon:city lat',
    create a dictionary where each unique integer latitude forms an entry (aka,
    bucket) and each integer longitude forms an entry for each latitude entry
    (another bucket). Return the dictionary.

    @param filename    File to retrieve city id, longitude, and latitude data

    @return a dictionary with the first key being latitudes, the second key
    being longitudes, and the value being a city id
    """

    # Dictionary to construct with 2 levels of keys, first level being
    # latitude and the second level being longitude
    coord_buckets = {}

    with open(filename, 'r') as ids_and_coords:
        for line in ids_and_coords:

            # Expect each line in the file to have 3 colon separated fields as
            # described above
            city_id, city_lon, city_lat = line.strip().split(':')

            # Converting the keys to int makes the dictionary easier to iterate
            city_lon = int(float(city_lon))
            city_lat = int(float(city_lat))

            # Check for an existing entry, if there is none then create one
            if coord_buckets.get(city_lat) is None:
                coord_buckets[city_lat] = {}

            # The value for each latitude, longitude key pair will be a list of
            # city ids for cities with the same coordinate pair
            if coord_buckets[city_lat].get(city_lon) is None:
                coord_buckets[city_lat][city_lon] = list()

            coord_buckets[city_lat][city_lon].append(city_id)

    return coord_buckets


def generate_id_list_from_file(filename='saved_city_ids.txt'):
    """
    Create and return a list of ids using the file indicated by the given
    filename. The file is expected to have format of a single integer per line.

    @param filename    Name of the file from which a list of ids will be created

    @return a list of city ids (integers)
    """

    file_as_list = None

    with open(filename, 'r') as f:
        file_as_list = [int(line) for line in f]

    return file_as_list


def produce_messages_with_n_workers(num_workers=2):
    """
    """
    with open('keys.txt', 'r') as f:
        api_keys = [key.strip() for key in f]
    with open('ids.txt', 'r') as f:
        city_ids = [city_id.strip() for city_id in f]
    api_handler = Api_Handler(api_keys)
    ids_per_worker = int(len(city_ids) / num_workers) + 1
    threads = list()

    for worker_id in range(0, num_workers):
        min_index = ids_per_worker * worker_id
        max_index = min(len(city_ids), (worker_id + 1) * ids_per_worker)
        id_subset = city_ids[min_index:max_index]
        thread = Api_Worker(id_subset, api_handler, producer)
        thread.start()
        threads.append(thread)

    for thread in threads:
        thread.join()


if __name__ == '__main__':
    produce_messages_with_n_workers()

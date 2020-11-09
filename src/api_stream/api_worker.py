import threading
from datetime import datetime


class Api_Worker(threading.Thread):

    # Kafka topic where temperature messages should be posted
    KAFKA_TOPIC = 'currentTemp'

    def __init__(self, city_ids, api_handler, producer):
        super(Api_Worker, self).__init__()
        self.city_ids = city_ids
        self.api_handler = api_handler
        self.kafka_producer = producer

    def produce_kafka_message(self, api_result):
        """
        From the result of a weather API call, produce a message to a Kafka
        topic with key of the format
        'city name:city longitude:city latitude:hour of day' and value
        'current city temperature'.

        @param api_result    Result of an API call, used to produce a message
        """
        # Extract important the values of a city we care about from the API
        # result and convert to a string where necessary for evaluation as a
        # single string later on
        city_name = api_result['name']
        city_temp = str(api_result['main']['temp'])
        city_lon = str(api_result['coord']['lon'])
        city_lat = str(api_result['coord']['lat'])
        # From the date that the API response was created, we only want the hour
        # of the day, from 0 to 23
        result_hour = str(datetime.fromtimestamp(api_result['dt']).hour)
        # Join values to construct a key (described above) using colons,
        # transform to bytes as required by the KafkaProducer send function
        msg_key = str.encode(
            ':'.join([city_name, city_lon, city_lat, result_hour]))
        msg_val = str.encode(city_temp)
        # Send a message to Kafka
        self.kafka_producer.send(self.KAFKA_TOPIC, key=msg_key, value=msg_val)

    def run(self):
        """
        Given a list of city ids, make API calls for each ID in order to
        retrieve current city weather information and then produce a Kafka
        message from that information.
        """
        index = 0
        while True:
            # Set the id to that of the current city in the request parameters
            # used to make an API call
            weather = self.api_handler.get_weather_info(self.city_ids[index])
            # If the weather_info is not None (API call was successful), produce
            # a Kafka message
            if weather:
                self.produce_kafka_message(weather)
            index = (index + 1) % len(self.city_ids)

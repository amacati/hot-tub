import threading
import requests
import time


class Api_Handler():
    """
    Class for handling calls to the Open Weather API from multiple threads at a
    time. Will automatically maintain a minutely call limit to prevent our
    account with Open Weather from being suspended.
    """
    # How many calls can be made by each API key per minute
    CALLS_PER_KEY = 60
    # The URL to send API requests
    URL = 'https://api.openweathermap.org/data/2.5/weather?'
    # The status code required to accept an API result
    EXPECTED_API_STATUS_CODE = 200

    def __init__(self, api_keys):
        self.api_keys = api_keys
        self.max_calls_per_minute = Api_Handler.CALLS_PER_KEY * len(api_keys)
        self.api_key_index = 0
        self.calls_made = {}
        self.api_key_index_lock = threading.Lock()
        self.calls_made_lock = threading.Lock()
        self.failed_counter = 0

    def get_weather_info(self, city_id):
        """
        Round-robin pick an API key from the list given on instantiation and
        make an API call for the given city ID. Return the result of the call.

        @param city_id    ID of the city to retrieve weather information

        @return the result of the API call (or none if the call failed)
        """
        api_key = 0
        # Acquire lock to shared API key index called api_key_index
        self.api_key_index_lock.acquire()
        api_key = self.api_keys[self.api_key_index]
        self.api_key_index = (self.api_key_index + 1) % len(self.api_keys)
        # Release lock to shared API key index called api_key_index
        self.api_key_index_lock.release()
        # Parameters used by the API call to specify what information is being
        # asked for
        request_parameters = {
            'id': city_id,
            'appid': api_key,
            'units': 'metric'
        }
        result = self.make_call_with_retries(request_parameters)
        return result

    def make_call_with_retries(self, request_params, num_retries=3):
        """
        Attempt and API call until successful or until the retry limit has been
        reached. A successful API call is one that has a status code of 200
        (AKA EXPECTED_API_STATUS_CODE). Return the result of the API call or
        None if the call failed to succeed with num_retries retries.

        @param request_params    parameters to send as part of the API call

        @return the result of the API call or None if the call failed too many
                times
        """
        for _ in range(0, num_retries):
            # Increment how many calls have been made this minute to keep track
            # of rate limiting
            self.increment_calls_per_minute()
            try:
                api_result = requests.get(Api_Handler.URL, params=request_params)
            except:  # noqa: E722
                self.failed_counter += 1
                print(f'Request failed! Total request failures: {self.failed_counter}', flush=True)
                continue
            if api_result.status_code == Api_Handler.EXPECTED_API_STATUS_CODE:
                return api_result.json()
        return

    def increment_calls_per_minute(self):
        """
        This is the function with all the logic for rate limiting our API calls
        and sleeping when appropriate. Use a dictionary - with a single entry
        for the current minute - to keep track of how many calls have been made
        for the current minute. If there is not an entry in the dictionary for
        the current minute, then reset the dictionary and add a new entry. If
        the count for the current minute is equal to the max calls per minute,
        sleep until the next minute. Otherwise, increment count and return.
        """
        added = False
        while not added:
            time_to_delay = 0
            curr_min = int(time.time() / 60)
            # Acquire lock to shared list calls_made
            self.calls_made_lock.acquire()
            curr_calls_made = self.calls_made.get(curr_min)
            # No calls made for the current minute, add entry for current minute
            if curr_calls_made is None:
                self.calls_made = {curr_min: 0}
                curr_calls_made = 0
            # Have not reached max calls, go ahead and increment
            if curr_calls_made < self.max_calls_per_minute:
                self.calls_made[curr_min] = curr_calls_made + 1
                added = True
            # Max calls reached, calculate how long to wait until next minute
            else:
                time_to_delay = ((curr_min + 1) * 60) - time.time()
            # Release lock to shared list calls_made
            self.calls_made_lock.release()
            # if needed, sleep until the next minute
            if time_to_delay > 0:
                time.sleep(int(time_to_delay) + 1)

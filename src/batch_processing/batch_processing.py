# -*- coding: utf-8 -*-

import os
import numpy as np
from pathlib import Path
import time
from scipy.interpolate import griddata
from scipy.ndimage.filters import gaussian_filter
import matplotlib.pyplot as plt
import seaborn as sns

from datetime import datetime
from pyspark import SparkContext
from pyspark.sql import SQLContext
from cassandra.cluster import Cluster
from reference_model.reference_model_loader import ReferenceModelLoader


class BatchProcessor:
    """!@brief Node to process the Cassandra table and put together a temperature difference model.

    Initializes a SparkSession and continuously executes the table processing. Data is read from Cassandra.
    @see https://cassandra.apache.org/.
    """

    def __init__(self):
        """!@brief BatchProcessor constructor.

        Exports necessary environment variables and initializes the SparkContext and SparkSQLContext.
        """
        self.ROOT_PATH = Path(__file__).resolve().parents[2]
        self.msg_count = 0
        self.num_api_workers = 1
        self.key_space_name = 'hot_tub'
        self.table_name = 'current'
        self.processing_sigma = 20  # Value for gaussian filtering during model postprocessing. Modify if necessary.
        self.land_mask = np.load(self.ROOT_PATH.joinpath('data', 'mask_model.npy'))
        self.reference_model_loader = ReferenceModelLoader()
        os.environ['PYSPARK_SUBMIT_ARGS'] = '--packages com.datastax.spark:spark-cassandra-connector_2.11:2.3.0 \
                                             --conf spark.cassandra.connection.host=127.0.0.1 pyspark-shell'
        self.sc = SparkContext()
        self.sql_context = SQLContext(self.sc)
        cluster = Cluster()
        session = cluster.connect()
        session.execute("CREATE KEYSPACE IF NOT EXISTS hot_tub WITH REPLICATION = {'class': 'SimpleStrategy', \
                        'replication_factor': 1};")
        session.execute("CREATE TABLE IF NOT EXISTS hot_tub.current (city_and_loc text, time int, temperature float, \
                        PRIMARY KEY (city_and_loc, time));")

    def start(self):
        """!@brief Starts the batch processing loop.

        Regularly runs an interpolation update on the currently available data in cassandra and saves it for the web
        server at /src/webserver/static.
        """
        while True:
            print("Starting batch processing", flush=True)
            self.batch_processing()
            print("Finished batch processing", flush=True)
            time.sleep(10)

    def batch_processing(self):
        """!@brief Processes the table data from Cassandra, creates a model from it and saves it.

        Reduces the city temperatures from the last 24 hours and averages them. Uses this average data to interpolate
        global data. Creates a temperature difference map by substracting the reference model from the current day and
        saves it to /data/global_temp_diff_map.png for use in the webserver."""
        cities = self._load_rdd()
        avg_by_city = cities.mapValues(lambda v: (v, 1)).reduceByKey(lambda a, b: (a[0]+b[0], a[1]+b[1])) \
                                                        .mapValues(lambda v: v[0]/v[1]).collectAsMap()
        curr_model = self.create_model(avg_by_city)
        day = datetime.now().timetuple().tm_yday - 1  # Starting at index 0.
        reference_model = self.reference_model_loader.load_model(day)
        model = curr_model - reference_model
        fig, ax = plt.subplots(figsize=(20, 10))
        ax.set_title('Current temperature difference compared to preindustrial times.')
        cmap = sns.color_palette("coolwarm", as_cmap=True)
        plot = sns.heatmap(model, ax=ax, vmin=-10, vmax=10, xticklabels=False, yticklabels=False, cmap=cmap,
                           cbar_kws={'label': 'Temperature difference in °C'})
        plot.get_figure().savefig(self.ROOT_PATH.joinpath('src', 'webserver', 'static', 'global_temp_diff_map.png'))
        plt.close(fig)

    def _load_rdd(self):
        """!@brief Returns the Cassandra table as an RDD.

        Table is processed to contain only the relevant tuple (city, temperature).

        @return The Cassandra table as SparkRDD.
        """
        rdd = self.sql_context.read.format("org.apache.spark.sql.cassandra").options(table=self.table_name,
                                                                                     keyspace=self.key_space_name) \
                                   .load().rdd.map(list).map(lambda x: (x[0], x[2]))
        return rdd

    def create_model(self, data):
        """!@brief Prepares the locations/values and triggers interpolation and saving of the model.

        Reads all cities and creates coordinate/value arrays from the cache or the geolocator.
        @return Returns the current interpolated model.
        """
        points = list()
        values = list()
        for city_and_loc, temperature in data.items():
            _, lon, lat = self._unpack_city(city_and_loc)
            points.append([lat, lon])
            values.append(temperature)
        return self._interpolate_model(points, values)

    @staticmethod
    def _unpack_city(city_and_loc):
        """!@brief Unpacks and converts the key into city, longitude and latitude.

        @return Returns the city name as string, longitude and latitude as floats."""
        split = city_and_loc.split(':')
        return split[0], float(split[1]), float(split[2])

    def _interpolate_model(self, points, values):
        """!@brief Interpolates and saves the model.

        Creates a nearest neighbor interpolation with gaussian blur, applies the land mask to the model and returns it.
        @return Returns the current interpolated model.
        """
        x_grid, y_grid = np.mgrid[0:900, 0:1800]
        points = np.array(points)*5  # Upscaling from 90/180° to 450/900
        values = np.array(values)
        print(f"Current number of points used for interpolation: {points.shape}", flush=True)
        if points.size == 0:
            return np.zeros((900, 1800))
        points_tf = np.zeros(points.shape)
        points_tf[:, 0] = points[:, 0] * -1 + 450
        points_tf[:, 1] = points[:, 1] + 900
        model = griddata(points_tf, values, (x_grid, y_grid), method='nearest')
        model = gaussian_filter(model, [self.processing_sigma, self.processing_sigma]).astype(np.float64)
        model[~self.land_mask] = np.nan
        return model


if __name__ == '__main__':
    batch_processor = BatchProcessor()
    batch_processor.start()

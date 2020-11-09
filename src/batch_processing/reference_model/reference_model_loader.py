"""!@brief Interface class to load the global reference temperature matrix.
@details Matrix is always of size (900, 1800). Matrix element (0,0) lies in the top left corner.
@file reference_model_loader.py ReferenceModelLoader class file.
@author Martin Schuck
@date 15.10.2020
"""

import numpy as np
import matplotlib.pyplot as plt
from pathlib import Path
import pandas as pd
from scipy.interpolate import griddata


class ReferenceModelLoader:
    """!@brief This class is responsible for loading the global reference temperature.

    Main functionality lies in the @ref load_model function. Plotting of models is only supported for debugging.
    """
    ROOT_PATH = Path(__file__).resolve().parents[3]  # Repository root path.

    def __init__(self):
        """!@brief Class constructor loads the land mask matrix into memory for fast access. 
        
        Larger dataset historical_average.csv is dropped during each load to save memory.
        """
        self.land_mask = np.load(self.ROOT_PATH.joinpath('data', 'mask_model.npy'), allow_pickle=False)
        self.model = dict()

    def load_model(self, day):
        """!@brief Loads the global reference temperature matrix from the cache.

        @param day Day of the year to load (from 0 to 364). Type int.
        @return Returns the global reference temperature matrix of shape (900,1800).
        """
        if day not in self.model.keys():
            self._load_model(day)
        return self.model[day]

    def _load_model(self, day):
        """!@brief Loads the global reference temperature matrix into the model cache.

        Throws an assertion error if the day is not present in the data frame.

        @param day Day of the year to load (from 0 to 364). Type int.
        """
        model_df = pd.read_csv(self.ROOT_PATH.joinpath('data', 'historical_average.csv'))
        assert(day in model_df['day_number'].unique())
        df = model_df[model_df['day_number'] == day]

        x_grid, y_grid = np.mgrid[-450:450, -900:900]
        points = np.vstack((df.latitude * 5, df.longitude * 5)).T
        values = df.climatology
        model = np.flipud(griddata(points, values, (x_grid, y_grid), method='cubic'))
        model[~self.land_mask] = np.nan
        self.model[day] = model
        del model_df

    def plot_model(self, day):
        """!@brief Plots the global reference temperature model.

        @param day Day of the year to load (from 0 to 364). Type int.
        """
        plt.subplot()
        plt.imshow(self.load_model(day=day), extent=(0, 2, 0, 1))
        plt.title('Base model')
        plt.gcf().set_size_inches(18, 18)
        plt.show()

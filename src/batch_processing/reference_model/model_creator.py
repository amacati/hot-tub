"""!@brief Model creator tool to generate a pre industrial average temperature model.
@details The model is extracted from the BerkeleyEarth dataset. More specific, we use the Daily Land
(Experimental; 1880 – Recent) Average Temperature (TAVG) 1º x 1º Latitude-Longitude Grid data set. Running this script
assumes that the data is present at /data/Complete_TAVG_Daily_LatLong1_1880.nc. To keep the repository size small, the
dataset is not included in the files. It can be downloaded from http://berkeleyearth.org/archive/data/.
@file model_creator.py Reference model creator file.
@author Martin Schuck
@date 14.10.2020
"""

from pathlib import Path
import xarray


ROOT_PATH = Path(__file__).resolve().parents[3]  # Repository root path.


def load_dataset():
    """!@brief Loads the data set from /data/Complete_TAVG_Daily_LatLong1_1880.nc.

    @return The Berkeley Earth data as an xarray data set.
    """
    try:
        data_path = ROOT_PATH.joinpath('data', 'Complete_TAVG_Daily_LatLong1_1880.nc')
        ds = xarray.open_dataset(data_path)
        return ds
    except FileNotFoundError:
        raise


def save_models(ds):
    """!@brief Saves the model as a pandas data frame.

    The reference model consists of longitude/latitude grid with the respective average temperature values in °C. Areas 
    without temperature information are not included to reduce model size. The model gets saved to 
    /data/historical_average.csv.
    @note The generated file size for the reference temperature model is quite large with > 200 MB.
    """
    save_path = ROOT_PATH.joinpath('data', 'historical_average.csv')
    df = ds.climatology.to_dataframe()
    df = df.climatology.dropna()
    df.to_csv(save_path)


if __name__ == '__main__':
    try:
        ds = load_dataset()
        save_models(ds)
    except FileNotFoundError:
        print("Dataset not found. Please make sure the data set is located at the described file location!")

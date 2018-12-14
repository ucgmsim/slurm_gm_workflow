#!/usr/bin/env python3
import json
import numpy as np
import keras

from typing import Tuple
from sklearn.model_selection import train_test_split


class WCEstModel(object):

    def __init__(self, model_config: str = None):
        self.is_trained = False
        self._model_config = model_config

    def train(self, X: np.ndarray, y: np.ndarray, val_split: float = None,
              val_data: Tuple[np.ndarray, np.ndarray] = None):
        """Performs training of the model, if either val_split or val_data
        then validation is also performed and displayed (i.e. validation loss)

        Parameters
        ----------
        X: numpy matrix with shape [n_samples, n_features]
            Input data to use for training/(validation) of the model
        y: numpy array with shape [n_samples]
            Associated labels for the input data
        val_split: float
            Proportion of the input_data to use for validation
        val_data: tuple of numpy arrays, (X_val, y_val)
            The data to use for validation, will overwrite val_split if both
            are set.
            X_val has to have same number of columns as the input_data.
        """
        raise NotImplementedError()

    def predict(self, X: np.ndarray):
        """Performs the actual prediction using the current model

        Parameters
        ----------
        X: numpy matrix with shape [n_samples, n_features]
            Input data to use for prediction

        Returns
        -------
        Returns a numpy vector of length n_samples, which contains the
        target predictions.
        """
        raise NotImplementedError()

    def save_model(self, output_file: str):
        """Saves a model"""
        raise NotImplementedError()

    @staticmethod
    def from_saved_model(self, model_file: str):
        """Loads a model from the specified file"""
        raise NotImplementedError()


class NNWcEstModel(WCEstModel):

    # Required fields in the config
    units_const = "units"
    dropout_const = "dropout"
    activation_const = "activation"

    n_epochs_const = "n_epochs"

    def __init__(self, model_config_file: str = None):
        """Sets the model parameters from the config file

        Parameters
        ----------
        model_config_file: str
            Path to the json model config to use.
        """
        super().__init__(model_config_file)

        self.config_dict = None
        if model_config_file is not None:
            with open(model_config_file, "r") as f:
                self.config_dict = json.load(f)

        self._model, self.hist = None, None
        self.train_data, self.val_data = None, None

    def train(self, X: np.ndarray, y: np.ndarray, val_split: float = None,
              val_data: Tuple[np.ndarray, np.ndarray] = None):
        """Trains the model, see WCEstModel.train for full doc"""
        if self.is_trained:
            raise Exception("This model has already been trained. "
                            "Each instance can only be trained once.")

        X_train, y_train = X, y

        # Check if input data needs to be split into train/val set
        if val_split is not None and val_data is None:
            X_train, X_val, y_train, y_val = train_test_split(
                X, y, test_size=val_split)
            val_data = (X_val, y_val)

        # Build the model architecture
        model = self._build_model(X.shape[1])

        # Optimizer
        model.compile(optimizer="adam", loss="mse")

        # Train the model
        self.hist = model.fit(X_train, y_train, epochs=10,
                         validation_data=val_data)

        self.train_data, self.val_data = (X_train, y_train), val_data
        self._model, self.is_trained = model, True

    def _build_model(self, input_dim: int):
        """Builds the keras model architecture"""
        # Load the model specifications
        if self._model_config is None:
            raise Exception("A model configuration has to be specified,"
                            "unless loading from an existing save model, "
                            "in which case from_saved_model should be used.")

        units = self.config_dict[self.units_const]
        dropout = self.config_dict[self.dropout_const]
        activation = self.config_dict[self.activation_const]

        # Build the model
        model = keras.models.Sequential()

        for ix, cur_n_unit in enumerate(units):
            if ix == 0:
                model.add(keras.layers.Dense(cur_n_unit,
                                             activation=activation,
                                             input_dim=input_dim))
            else:
                model.add(keras.layers.Dense(cur_n_unit,
                                             activation=activation))

            # Add dropout
            if dropout is not None:
                model.add(keras.layers.Dropout(dropout))

        # Add the output layer
        model.add(keras.layers.Dense(1))

        return model

    def predict(self, X: np.ndarray):
        """Performs the actual prediction using the current model
        For full doc see WCEstModel.predict
        """
        if not self.is_trained:
            raise Exception("This model has not been trained!")

        return self._model.predict(X)

    def save_model(self, output_file: str):
        """Saves the model in a hdf5 file"""
        if not self.is_trained:
            raise Exception("This model has not been trained!")

        self._model.save(output_file)

    def load_model(self, model_file: str):
        """Loads the full keras model (architecture + weights) from
        the specified hdf5 file
        """
        if self.is_trained:
            raise Exception("This model has already been trained!")

        self._model = keras.models.load_model(model_file)

    @staticmethod
    def from_saved_model(self, model_file: str):
        """Creates a new nn model instance from a save file"""
        nn_model = NNWcEstModel()
        nn_model.load_model(model_file)

        return nn_model







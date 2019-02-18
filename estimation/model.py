#!/usr/bin/env python3
import json
from typing import Tuple

import numpy as np
import keras
import h5py
from sklearn.model_selection import train_test_split


class WCEstModel(object):
    def __init__(self, model_config: str = None):
        self.is_trained = False
        self._model_config = model_config

    def train(
        self,
        X: np.ndarray,
        y: np.ndarray,
        val_split: float = None,
        val_data: Tuple[np.ndarray, np.ndarray] = None,
    ):
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
        The data has to have been scaled (with the same parameters/scaler
        as used for training)

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

    def load_model(self, model_file: str):
        """Loads a model from the specified model file"""
        raise NotImplementedError()

    @classmethod
    def from_saved_model(cls, model_file: str):
        """Loads a model from the specified file"""
        model = cls()
        model.load_model(model_file)

        return model


class NNWcEstModel(WCEstModel):

    # Required fields in the config
    units_const = "units"
    dropout_const = "dropout"
    activation_const = "activation"

    n_epochs_const = "n_epochs"
    loss_const = "loss"
    metrics_const = "metrics"

    def __init__(self, model_config_file: str = None):
        """Sets the model parameters from the config file

        Parameters
        ----------
        model_config_file: str
            Path to the json model config to use.
        """
        super().__init__(model_config_file)

        self._config_dict = None
        if model_config_file is not None:
            with open(model_config_file, "r") as f:
                self._config_dict = json.load(f)

        self._model, self.hist = None, None
        self.train_data, self.val_data = None, None

        self._train_min, self._train_max = None, None

    def train(
        self,
        X: np.ndarray,
        y: np.ndarray,
        val_split: float = None,
        val_data: Tuple[np.ndarray, np.ndarray] = None,
    ):
        """Trains the model, see WCEstModel.train for full doc"""
        if self.is_trained:
            raise Exception(
                "This model has already been trained. "
                "Each instance can only be trained once."
            )

        X_train, y_train = X, y

        # Save the min/max for each feature
        self._train_min = X_train.min(axis=0)
        self._train_max = X_train.max(axis=0)

        # Check if input data needs to be split into train/val set
        if val_split is not None and val_data is None:
            X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=val_split)
            val_data = (X_val, y_val)

        # Build the model architecture
        model = self._build_model(X.shape[1])

        # Optimizer
        loss_name = self._config_dict[self.loss_const]
        model.compile(
            optimizer="adam",
            loss=loss_name,
            metrics=self._config_dict[self.metrics_const],
        )

        # Train the model
        self.hist = model.fit(
            X_train,
            y_train,
            epochs=self._config_dict[self.n_epochs_const],
            validation_data=val_data,
        )

        print(
            "Trained the model using {} samples giving a "
            "{} loss of {:.5f} for the final epoch.".format(
                X_train.shape[0], loss_name, self.hist.history["loss"][-1]
            )
        )

        if val_data is not None:
            print(
                "Validated the model using {} samples giving a "
                "{} loss of {:.5f} for the final epoch.".format(
                    val_data[0].shape[0], loss_name, self.hist.history["val_loss"][-1]
                )
            )

        self.train_data, self.val_data = (X_train, y_train), val_data
        self._model, self.is_trained = model, True

    def _build_model(self, input_dim: int):
        """Builds the keras model architecture"""
        # Load the model specifications
        if self._model_config is None:
            raise Exception(
                "A model configuration has to be specified,"
                "unless loading from an existing save model, "
                "in which case from_saved_model should be used."
            )

        units = self._config_dict[self.units_const]
        dropout = self._config_dict[self.dropout_const]
        activation = self._config_dict[self.activation_const]

        # Build the model
        model = keras.models.Sequential()

        for ix, cur_n_unit in enumerate(units):
            if ix == 0:
                model.add(
                    keras.layers.Dense(
                        cur_n_unit, activation=activation, input_dim=input_dim
                    )
                )
            else:
                model.add(keras.layers.Dense(cur_n_unit, activation=activation))

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

        if np.any(X > self._train_max) or np.any(X < self._train_min):
            print(
                "WARNING: Some of the data specified for estimation exceeds the "
                "limits of the data the model was trained. This will result in "
                "incorrect estimation!"
            )

        return self._model.predict(X)

    def save_model(self, output_file: str):
        """Saves the model in a hdf5 file"""
        if not self.is_trained:
            raise Exception("This model has not been trained!")

        self._model.save(output_file)

        with h5py.File(output_file, "r") as f:
            f["custom"] = np.concatenate(
                (self._train_min[None, :], self._train_max[None, :]), axis=0
            )

    def load_model(self, model_file: str):
        """Loads the full keras model (architecture + weights) from
        the specified hdf5 file
        """
        if self.is_trained:
            raise Exception("This model has already been trained!")

        self._model = keras.models.load_model(model_file)
        self.is_trained = True

        with h5py.File(model_file, "r+") as f:
            self._train_min = f["custom"][0, :]
            self._train_max = f["custom"][1, :]


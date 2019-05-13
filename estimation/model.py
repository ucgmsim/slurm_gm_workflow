#!/usr/bin/env python3
import os
import json
import pickle
from logging import Logger
from typing import Tuple

import numpy as np
import keras
import h5py
from sklearn.model_selection import train_test_split, KFold
from sklearn.preprocessing import StandardScaler
from sklearn.svm import SVR

import qcore.constants as const
from shared_workflow.workflow_logger import get_basic_logger, NOPRINTCRITICAL


def mre(y, y_est, sample_weights=None):
    """Mean relative error"""
    if sample_weights is None:
        return np.mean((y_est - y) / y)
    else:
        return np.mean(((y_est - y) / y) * sample_weights)


def mare(y, y_est, sample_weigths=None):
    """Mean absolute relative error"""
    if sample_weigths is None:
        return np.mean(np.abs(y_est - y) / y)
    else:
        return np.mean((np.abs(y_est - y) / y) * sample_weigths)


class WCEstModel(object):
    """Base class for core hours estimation models"""

    def __init__(self, model_config: str = None):
        self.is_trained = False
        self._model_config = model_config

    def train(
        self,
        X: np.ndarray,
        y: np.ndarray,
        val_split: float = None,
        val_data: Tuple[np.ndarray, np.ndarray] = None,
        **kwargs,
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

    def predict(self, X: np.ndarray, logger: Logger =None):
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
    def evaluate(
        cls, model_config_file: str, X: np.ndarray, y: np.ndarray, n_folds: int = 3
    ):
        """Evaluates the model using K-fold

        Returns list with n_fold entries, of the format
        ((y_train, y_est_train), (y_test, y_est_test))
        """
        kfold = KFold(n_splits=n_folds, shuffle=True)

        result = []
        for train, test in kfold.split(X, y):
            model = cls(model_config_file)
            model.train(X[train], y[train], verbose=2)
            y_est_train = model.predict(X[train])
            y_est_test = model.predict(X[test])

            result.append(((y[train], y_est_train), (y[test], y_est_test)))

        return result

    @classmethod
    def from_saved_model(cls, model_file: str):
        """Loads a model from the specified file"""
        model = cls()
        model.load_model(model_file)

        return model


class NNWcEstModel(WCEstModel):
    """Represents Neural Network for core hour estimation, using keras/tensorflow
    in the background.
    """

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

    def __get_model_str(self):
        """Creates a name containing the models config, used for logging."""
        return "{}-{}-BS_{}-DR_{}-ACT_{}_UN_{}".format(
            const.timestamp,
            self._config_dict["loss"],
            self._config_dict["batch_size"],
            self._config_dict["dropout"],
            self._config_dict["activation"],
            "_".join([str(n) for n in self._config_dict["units"]]),
        ).replace(".", "p")

    def train(
        self,
        X: np.ndarray,
        y: np.ndarray,
        val_split: float = None,
        val_data: Tuple[np.ndarray, np.ndarray] = None,
        verbose: int = 1,
        **kwargs,
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

        callbacks = []
        debug_dir = kwargs.get("debug_dir")
        if debug_dir is not None:
            callbacks.append(
                keras.callbacks.TensorBoard(
                    log_dir=os.path.join(debug_dir, self.__get_model_str())
                )
            )

        # Train the model
        self.hist = model.fit(
            X_train,
            y_train,
            epochs=self._config_dict[self.n_epochs_const],
            validation_data=val_data,
            callbacks=callbacks,
            verbose=verbose,
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

    def predict(self, X: np.ndarray, warning: bool = True, logger: Logger = get_basic_logger()):
        """Performs the actual prediction using the current model

        For full doc see WCEstModel.predict
        """
        if not self.is_trained:
            logger.log(NOPRINTCRITICAL, "There was an attempt to use an untrained model")
            raise Exception("This model has not been trained!")

        if np.any(self.get_out_of_bounds_mask(X)) and warning:
            print(
                "WARNING: Some of the data specified for estimation exceeds the "
                "limits of the data the model was trained. This will result in "
                "incorrect estimation!"
            )

        return self._model.predict(X).reshape(-1)

    def get_out_of_bounds_mask(self, X: np.ndarray):
        """Checks that the input data is within the bounds of the data
        used for training of the neural network.

        Have to use isclose due to minor floating point differences.
        """
        return (
            (X > self._train_max)
            & ~np.isclose(X, np.repeat(self._train_max.reshape(1, 5), X.shape[0], axis=0))
        ) | (
            (X < self._train_min)
            & ~np.isclose(X, np.repeat(self._train_min.reshape(1, 5), X.shape[0], axis=0))
        )

    def save_model(self, output_file: str):
        """Saves the model in a hdf5 file"""
        if not self.is_trained:
            raise Exception("This model has not been trained!")

        self._model.save(output_file)

        with h5py.File(output_file, "r+") as f:
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

        with h5py.File(model_file, "r") as f:
            self._train_min = f["custom"][0, :]
            self._train_max = f["custom"][1, :]

    @staticmethod
    def preprocessing(
        X: np.ndarray,
        y: np.ndarray,
        std_scaler: StandardScaler = None,
        scaler_save_file: str = None,
    ):
        """Performs the preprocessing

        Standardizes the input data (i.e. mean=0, std=1) for each feature.
        Removes any rows which contain a np.nan entry
        Saves scaler if file is specified

        Returns
        -------
        Scaled X, y, StandardScaler instance used
        """
        # Remove nan entries
        row_nan_mask = np.any(np.isnan(X), axis=1) | np.isnan(y)
        X, y = X[~row_nan_mask, :], y[~row_nan_mask]

        # Scale
        if std_scaler is not None:
            X = std_scaler.transform(X)
        else:
            std_scaler = StandardScaler()
            X = std_scaler.fit_transform(X)

        if scaler_save_file is not None:
            with open(scaler_save_file, "wb") as f:
                # Have to use protocol 2 so it can be loaded in python2
                pickle.dump(std_scaler, f, protocol=2)

        return X, y, std_scaler


class SVRModel(WCEstModel):
    """Represents Support Vector Machine used for core hours estimation. Specifically,
    for estimation for features that are out of bounds for the neural network.

    SVR == Support Vector Regression
    """

    # Required fields in the config
    C = "C"
    gamma = "gamma"
    y_threshold = "y_threshold"

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

        self._model = None

    def train(
        self,
        X: np.ndarray,
        y: np.ndarray,
        val_split: float = None,
        val_data: Tuple[np.ndarray, np.ndarray] = None,
        **kwargs,
    ):
        """
        Training of the SVR, for the full train doc see WCEstModel.

        Due to the imbalance in small and large trainings samples, weights are added to
        all sources above the y_threshold, so that their combined weight is equal to the
        combined weight of all sources than the threshold.

        Note: The errors calculated as part of the training (if specified) are not
        overly meaningful as they do not use the weights and are therefore
        dominated by the small training samples.
        """
        X_train, y_train = X, y

        # Check if input data needs to be split into train/val set
        if val_split is not None and val_data is None:
            X_train, X_val, y_train, y_val = train_test_split(X, y, test_size=val_split)

        # Calculate sample weights, to give all samples with core_hours above
        # the threshold  the same weight as all smaller ones. To allow addressing
        # of the inbalance in samples
        y_threshold = self._config_dict[self.y_threshold]
        sample_weights = np.ones(X_train.shape[0], dtype=np.float)
        if y_threshold:
            mask = y_train > y_threshold
            sample_weights[mask] = np.count_nonzero(~mask) / np.count_nonzero(mask)

        # Fit the model
        self._model = SVR(
            cache_size=1000,
            kernel="poly",
            degree=2.0,
            C=self._config_dict[self.C],
            gamma=self._config_dict[self.gamma],
        )
        self._model.fit(X_train, y_train, sample_weight=sample_weights)
        self.is_trained = True

        y_est_train = self._model.predict(X_train)
        print(
            "Training: Mean absolute relative error {}, "
            "Mean relative error {}".format(
                mare(y_train, y_est_train), mre(y_train, y_est_train)
            )
        )

        if val_split or val_data:
            y_est_val = self._model.predict(X_val)
            print(
                "Validation: Mean absolute relative error {}, "
                "Mean relative error {}".format(
                    mare(y_val, y_est_val), mre(y_val, y_est_val)
                )
            )

    def predict(self, X: np.ndarray, logger: Logger = get_basic_logger()):
        """Performs the actual prediction using the current model

        For full doc see WCEstModel.predict
        """
        if not self.is_trained:
            logger.log(NOPRINTCRITICAL, "There was an attempt to use an untrained model")
            raise Exception("This model has not been trained!")

        return self._model.predict(X).reshape(-1)

    def save_model(self, output_file: str):
        """Saves the model as a pickle object"""
        if not self.is_trained:
            raise Exception("This model has not been trained!")

        with open(output_file, "wb") as f:
            pickle.dump(self._model, f)

    def load_model(self, model_file: str):
        """Loads the model from the specified pickle file
        """
        if self.is_trained:
            raise Exception("This model has already been trained!")

        with open(model_file, "rb") as f:
            self._model = pickle.load(f)

        self.is_trained = True

    @staticmethod
    def preprocessing(
        X: np.ndarray,
        y: np.ndarray,
        std_scaler: StandardScaler = None,
        scaler_save_file: str = None,
    ):
        """Same preprocessing as for the neural network"""
        return NNWcEstModel.preprocessing(
            X, y, std_scaler=std_scaler, scaler_save_file=scaler_save_file
        )


class CombinedModel:
    """Used to represent a model that primarily uses a NN model for estimation,
    however if the input data is "out of bounds", a SVR is used for
    extrapolation."""

    def __init__(self, nn_model: NNWcEstModel, svr_model: SVRModel):
        """Loads the saved NN and SVR model

        Parameters
        ----------
        nn_model: NNWcEstModel
            NN model to use.
        svr_model: SVRModel
            SVR model to use.
        """
        self.nn_model = nn_model
        self.svr_model = svr_model

    def predict(
        self,
        X_nn: np.ndarray,
        X_svr: np.ndarray,
        n_cores: np.ndarray,
        default_n_cores: int,
        logger: Logger = get_basic_logger(),
    ):
        """Attempt to use the NN model for estimation, however if input data
        is out of bounds, use the SVR model

        Parameters
        ----------
        X_nn: array of floats, shape [number of entries, number of features]
            Input data for NN, last column has to be the number of cores
        X_svr: array of floats, shape [number of entries, number of features]
            Input data for SVR
        n_cores: array of integers
            The non-normalised number of cores (i.e. actual number of
            physical cores to estimate for)
        default_n_cores: int
            The default number of cores for the process type that is being estimated.
        logger: Logger
            Logger for messages to be logged against
        """
        assert X_nn.shape[0] == X_svr.shape[0]

        out_bound_mask = np.any(self.nn_model.get_out_of_bounds_mask(X_nn), axis=1)
        if np.all(~out_bound_mask):
            return self.nn_model.predict(X_nn, warning=False, logger=logger)
        else:
            if np.any(~out_bound_mask):
                # Identify all entries that are out of bounds

                # Estimate using NN
                results = np.ones(X_nn.shape[0], dtype=np.float) * np.nan
                results[~out_bound_mask] = self.nn_model.predict(
                    X_nn[~out_bound_mask, :], warning=False, logger=logger
                )

                # Estimate out of bounds using SVR
                logger.debug(
                    "Some entries are out of bounds, these will be "
                    "estimated using the SVR model."
                )
                results[out_bound_mask] = (
                    self.svr_model.predict(X_svr[out_bound_mask, :], logger=logger) * default_n_cores
                ) / n_cores[out_bound_mask]

                return results
            else:
                logger.debug(
                    "The entry is out of bounds. The SVR models will be "
                    "used for estimation."
                )
                return self.svr_model.predict(X_svr, logger=logger)

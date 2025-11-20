import pytest
import pandas as pd
import numpy as np
from scipy.stats import t
from keibaai.src.modules.models.sigma_estimator import SigmaEstimator
from keibaai.src.modules.models.nu_estimator import NuEstimator

@pytest.fixture
def synthetic_data():
    n_samples = 200
    n_races = 20
    
    X = pd.DataFrame({
        'feature1': np.random.rand(n_samples),
        'feature2': np.random.rand(n_samples)
    })
    
    race_ids = np.repeat(np.arange(n_races), n_samples // n_races)
    race_ids_str = [f"race_{i}" for i in race_ids]
    
    # True Parameters
    true_mu = 100 + 10 * X['feature1']
    true_sigma = 1.0 + 0.5 * X['feature2']
    true_nu = 5.0
    
    y = np.array([t.rvs(df=true_nu, loc=m, scale=s) for m, s in zip(true_mu, true_sigma)])
    mu_pred = true_mu + np.random.normal(0, 0.1, n_samples)
    
    return X, y, mu_pred, race_ids_str

def test_sigma_estimator_fit_predict(synthetic_data):
    X, y, mu_pred, _ = synthetic_data
    model = SigmaEstimator()
    model.fit(X, y, mu_pred)
    
    preds = model.predict(X)
    assert len(preds) == len(X)
    assert np.all(preds > 0) # Sigma must be positive
    
    # Check if model learned something (better than random)
    # Note: This is a weak test for synthetic data, but ensures basic functionality
    assert model.feature_names_ == ['feature1', 'feature2']

def test_nu_estimator_fit_predict(synthetic_data):
    X, y, mu_pred, race_ids = synthetic_data
    
    # Sigma prediction needed for Nu training
    sigma_model = SigmaEstimator()
    sigma_model.fit(X, y, mu_pred)
    sigma_pred = sigma_model.predict(X)
    
    # Prepare race features
    race_features = X.copy()
    race_features['race_id'] = race_ids
    race_features_grouped = race_features.groupby('race_id').mean().reset_index()
    
    model = NuEstimator()
    model.fit(race_features_grouped, y, mu_pred, sigma_pred, pd.Series(race_ids))
    
    preds = model.predict(race_features_grouped)
    assert len(preds) == len(race_features_grouped)
    assert np.all(preds > 2.0) # Nu > 2 constraint
    
def test_nu_estimator_mle():
    # Test internal MLE estimation
    model = NuEstimator()
    
    # Generate t-distributed data with known df=10
    known_nu = 10.0
    residuals = t.rvs(df=known_nu, size=1000)
    
    estimated_nu = model._estimate_nu_mle(residuals)
    
    # Allow some margin of error
    assert 8.0 < estimated_nu < 12.0

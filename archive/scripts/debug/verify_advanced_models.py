import pandas as pd
import numpy as np
import os
from keibaai.src.modules.models.sigma_estimator import SigmaEstimator
from keibaai.src.modules.models.nu_estimator import NuEstimator
from scipy.stats import t

def verify_models():
    print("Verifying Sigma and Nu Models...")
    
    # 1. Generate Synthetic Data
    n_samples = 1000
    n_races = 100
    
    # Features
    X = pd.DataFrame({
        'feature1': np.random.rand(n_samples),
        'feature2': np.random.rand(n_samples)
    })
    
    # Race IDs
    race_ids = np.repeat(np.arange(n_races), n_samples // n_races)
    race_ids_str = [f"race_{i}" for i in race_ids]
    
    # True Parameters
    true_mu = 100 + 10 * X['feature1']
    true_sigma = 1.0 + 0.5 * X['feature2'] # Sigma depends on feature2
    true_nu = 5.0 # Constant Nu for simplicity
    
    # Generate Target (Finish Time) using t-distribution
    y = np.array([t.rvs(df=true_nu, loc=m, scale=s) for m, s in zip(true_mu, true_sigma)])
    
    # Mock Mu Predictions (add some noise to simulate imperfect model)
    mu_pred = true_mu + np.random.normal(0, 0.1, n_samples)
    
    # 2. Test Sigma Estimator
    print("\nTesting SigmaEstimator...")
    sigma_model = SigmaEstimator()
    sigma_model.fit(X, y, mu_pred)
    
    sigma_pred = sigma_model.predict(X)
    print(f"True Sigma Mean: {true_sigma.mean():.4f}")
    print(f"Pred Sigma Mean: {sigma_pred.mean():.4f}")
    
    # Check if sigma prediction correlates with feature2 (as defined in true_sigma)
    correlation = np.corrcoef(sigma_pred, X['feature2'])[0, 1]
    print(f"Correlation with feature2 (should be positive): {correlation:.4f}")
    
    # 3. Test Nu Estimator
    print("\nTesting NuEstimator...")
    
    # Create Race Features (mean of horse features)
    race_features = X.copy()
    race_features['race_id'] = race_ids_str
    race_features_grouped = race_features.groupby('race_id').mean().reset_index()
    
    nu_model = NuEstimator()
    nu_model.fit(race_features, y, mu_pred, sigma_pred, pd.Series(race_ids_str))
    
    nu_pred = nu_model.predict(race_features_grouped)
    print(f"True Nu: {true_nu}")
    print(f"Pred Nu Mean: {nu_pred.mean():.4f}")
    
    print("\nVerification Complete.")

if __name__ == "__main__":
    verify_models()

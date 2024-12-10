import pandas as pd
import numpy as np
from scipy import linalg
import matplotlib.pyplot as plt
from statsmodels.nonparametric.kernel_regression import KernelReg
from sklearn.decomposition import PCA
from numpy.linalg import svd
from statsmodels.tsa.arima.model import ARIMA
from scipy.stats import boxcox

class natModel():
    def __init__(self, tfr, n_components, error):
        self.tfr = tfr
        self.n_components = n_components
        self.error = error
        self.num_age_groups = len(tfr.columns)
        self.num_years = len(tfr.index)
        self.start_year = tfr.index[0]
        self.averages = self.nat_averages()
        self.centered_data = self.centralized_frame()
    
    def nat_averages(self):
        aggregates = list(self.tfr.sum())
        averages = []
        for i in range(0, len(self.tfr.columns.values)):
            averages.append(float(aggregates[i] / self.tfr.index.size))
        return averages


    def centralized_frame(self):
        centered_matrix = pd.DataFrame(index = self.tfr.index)
        tfr_t = self.tfr.copy().T.reset_index(drop=True).T
        for i in range(len(self.averages)):
            centered_matrix[i] = pd.concat([pd.Series(tfr_t[i] - self.averages[i])], axis=1)
        return centered_matrix
    
    def year_effects(self):
        pca = PCA(n_components=self.n_components)
        year_effects = pd.DataFrame(pca.fit_transform(self.centered_data))
        return year_effects
        
    def age_effects(self):
        pca = PCA(n_components=self.n_components)
        age_effects = pd.DataFrame(pca.fit_transform(self.centered_data.T))
        return age_effects
        
    
    def project(self, n_years, p=0, d=1, q=0, ext_effects = None):
        if ext_effects != None:
            y_effects = ext_effects
        else:
            y_effects = self.year_effects()

        # Arima
        y_effects_f = pd.DataFrame()
        for i in y_effects.columns:
            model = ARIMA(y_effects[i], order=(p,d,q))
            model_fit = model.fit(method_kwargs={'maxiter':2000})
            y_effects_f[i] = pd.concat([pd.Series(model_fit.forecast(n_years))])
        y_effects_f = pd.concat([y_effects, y_effects_f], axis=0)
        return y_effects_f
    

    def forecasted_tfr(self):
        age_effects = self.age_effects()
        year_effects = self.year_effects()
        averages = self.averages
        print(averages)

        effects_1 = pd.DataFrame()
        effects_2 = pd.DataFrame()
        effects_3 = pd.DataFrame()
        effects_4 = pd.DataFrame()

        for i in age_effects.index:
            effects_1[i] = pd.concat([year_effects[0] * age_effects[0].iloc[i]])
            effects_2[i] = pd.concat([year_effects[1] * age_effects[1].iloc[i]])
            effects_3[i] = pd.concat([year_effects[2] * age_effects[2].iloc[i]])
            effects_4[i] = pd.concat([year_effects[3] * age_effects[3].iloc[i]])
        forecast_fertility_rates = effects_1 + effects_2 + effects_3 + effects_4
        for j in range(0, len(averages)):
            forecast_fertility_rates[j] =  forecast_fertility_rates[j] + averages[j]
        return forecast_fertility_rates


class dataTransform():
    def __init__(self, fem_pop, births):
        self.births = births
        self.fem_pop = fem_pop
        self.columns = fem_pop.columns
        self.tfr = self.fertility_rate()
        self.num_age_groups = len(fem_pop.columns)
        self.fit_data = self.box_cox_fit()
        self.fit_lambda = self.lambda_fit()

    def fertility_rate(self):
        tfr = pd.DataFrame(index=self.fem_pop.index)
        for i in self.columns:
            tfr[i] = pd.concat([self.births[i] / self.fem_pop[i]])
        return tfr
    
    def box_cox_fit(self):
        fitted_data = pd.DataFrame()
        for i in range(0, self.num_age_groups):
            fit, lamb = boxcox(self.tfr[self.columns[i]])
            fitted_data[i] = pd.concat([pd.Series(fit)], axis=1)
        fitted_data = fitted_data.set_index(self.tfr.index)
        return fitted_data
    
    def lambda_fit(self):
        fitted_lambda = pd.Series()
        for i in range(0, self.num_age_groups):
            fit, lamb = boxcox(self.tfr[self.columns[i]])
            fitted_lambda[i] = lamb
        return fitted_lambda

    def kernel_transform(self):
        num_age_groups = len(self.fit_data.columns)
        tfr_transform = pd.DataFrame()
        marginal_effects = pd.DataFrame()
        rsquared = pd.Series()
        for i in range(0, num_age_groups):
            model = KernelReg(self.fit_data[i], self.fit_data[i], var_type='c')
            tfr_hat, m_effects = model.fit()
            rsquared[i] = model.r_squared()
            tfr_transform[i] = pd.concat([pd.Series(tfr_hat)], axis=1)
            marginal_effects[i] = pd.concat([pd.DataFrame(m_effects)], axis=1)
        tfr_transform = tfr_transform.set_index(self.fit_data.index)
        return tfr_transform, marginal_effects, rsquared

    def errors(self):
        variance = pd.DataFrame(index=self.tfr.index)
        j = 0
        for i in self.columns:
            lambda_err = self.births[i].sum(axis=0) / self.fem_pop[i].sum(axis=0)
            exp = 2*lambda_err - 1
            variance[j] = pd.concat([(self.tfr[i]**exp)*(self.fem_pop[i].astype("float") ** -1)])
            j += 1

        return variance

def main():
    nat_data = pd.read_csv("births.csv").set_index("year")
    female_pop = pd.read_csv("fem_pop.csv").set_index("year")
    data_transformed = dataTransform(female_pop, nat_data)
    nat_model = natModel(data_transformed.tfr, 4, data_transformed.errors())
    #tfr_transform, marginal_effects, rsquared = dataTransform(nat_data).kernel_transform()
    #nat_model = natModel(tfr_transform, 4)

    #tfr_projection = nat_model.project(n_years=30)
    #tfr_projection.plot()
    #plt.show()
    #err = nat_model.centralized_frame() - pred_tfr
    #j = 0
    #for i in rsquared:
    #    print(f"R^2 of age group {j}: {i}")
    #    j =+ 1

    # Plot
    #fig, axes = plt.subplots(nrows = 1, ncols=3)
    #nat_model.centralized_frame().plot(ax=axes[0], title="Original Centralized TFR")
    #pred_tfr.plot(ax=axes[1], title="Kernel Regression TFR")
    #err.plot(ax=axes[2], title="Kernel Regression Errors vs Original Data")
    #plt.show()
    
if __name__ == "__main__":
    main()

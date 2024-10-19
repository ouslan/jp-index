import pandas as pd
import scipy
import numpy as np


class mortModel():
    def __init__(self, deaths, exposure):
        self.deaths = deaths
        self.exposure = exposure
        self.death_rate = self.deaths.divide(self.exposure)
        self.log_death_rate = np.log(self.death_rate)
    
    def mort_constants(self):
        log_aggregates = list(self.log_death_rate.T.sum())
        averages = []
        for i in range(0, self.log_death_rate.index.size):
            averages.append(float(log_aggregates[i] / len(self.log_death_rate.columns.values)))
        return averages
        
    def centralized_matrix(self):
        centered_matrix = pd.DataFrame(index = self.log_death_rate.columns.values)
        logged_rates = self.log_death_rate.copy()
        logged_rates = logged_rates.reset_index(drop=True)
        logged_rates = logged_rates.T
        for i in range(len(self.mort_constants())):
            centered_matrix[i] = pd.concat([pd.Series(logged_rates[i] - self.mort_constants()[i])], axis = 1)
        return centered_matrix.T
    

    def age_constants(self):
        matrix = self.centralized_matrix().to_numpy()
        U,S,V = np.linalg.svd(matrix)
        age_constant_list = pd.DataFrame(U)
        age_constant_list = pd.Series(age_constant_list.T.iloc[0])
        return age_constant_list
    

    def year_constants(self):
        matrix = self.centralized_matrix().to_numpy()
        U,S,V = np.linalg.svd(matrix)
        year_constants = pd.DataFrame(V)
        year_constants = pd.Series(year_constants.iloc[0])
        return year_constants
    
    def scaling_eigenvalue(self):
        matrix = self.centralized_matrix().to_numpy()
        U,S,V = np.linalg.svd(matrix)
        vector = list(S)
        return vector[0]
    
    def mortality_rate(self, projected_constants):
        mortality = pd.DataFrame()
        for i in range(0, len(projected_constants)):
            values_list = []
            for j in range(0,16):
                values_list.append(np.exp(self.mort_constants()[j] + (self.scaling_eigenvalue()*self.age_constants()[j]*projected_constants[i])))
                print(f"Age group {j} for year {i} done")
            mortality[i] = pd.concat([pd.Series(values_list)], axis=1)
            print(f"Year {i} done")
        return mortality


def main():
    deaths_male = pd.read_csv("deaths_male.csv").set_index("age_group")
    exposure_male = pd.read_csv("exposure_male.csv").set_index("age_group")
    lc = mortModel(deaths_male, exposure_male)


if __name__ == "__main__":
    main()
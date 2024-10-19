import pandas as pd
import scipy
import numpy as np


class mortModel():
    def __init__(self, deaths, exposure):
        self.deaths = deaths
        self.exposure = exposure
        self.death_rate = self.deaths.divide(self.exposure)
        self.log_death_rate = np.log(self.death_rate)
        self.mort_constants = self.log_death_rate.sum() / self.log_death_rate.index.size
        self.projected_year_constant = ...
        self.projected_death_rate = ...
        
    def centralized_matrix(self):
        centered_matrix = pd.DataFrame(index = self.log_death_rate.index)
        headers = list(self.log_death_rate.columns.values)
        for i in range(int(headers[0]), int(headers[-1])+1):
            centered_matrix[i] = pd.concat([pd.Series(self.log_death_rate[f"{i}"] - self.mort_constants.iloc[i-int(headers[0])])], axis = 1)
        return centered_matrix
    

    def age_constants(self):
        matrix = self.centralized_matrix().to_numpy()
        U,S,V = np.linalg.svd(matrix)
        age_constant_list = pd.DataFrame(U)
        return age_constant_list
    

    def year_constants(self):
        matrix = self.centralized_matrix().to_numpy()
        U,S,V = np.linalg.svd(matrix)
        year_constants = pd.DataFrame(V)
        return year_constants
    
    def scaling_eigenvalue(self):
        matrix = self.centralized_matrix().to_numpy()
        U,S,V = np.linalg.svd(matrix)
        scalar = S
        return scalar


def main():
    deaths_male = pd.read_csv("deaths_male.csv").set_index("age_group")
    exposure_male = pd.read_csv("exposure_male.csv").set_index("age_group")
    lc = mortModel(deaths_male, exposure_male)


if __name__ == "__main__":
    main()
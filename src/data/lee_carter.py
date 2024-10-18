import pandas as pd
import scipy
import numpy as np


class mortModel():
    def __init__(self, deaths, exposure):
        self.deaths = deaths
        self.exposure = exposure
        self.death_rate = self.deaths.divide(self.exposure)
        self.log_death_rate = np.log(self.death_rate)
        self.mort_constants = self.log_death_rate.sum() / len(self.log_death_rate.index.values.tolist())
        self.projected_year_constant = ...
        self.projected_death_rate = ...
        
    def centralized_matrix(self):
        centered_matrix = pd.DataFrame(index = self.log_death_rate.index)
        for i in range(1,17):
            centered_matrix[f"male_{i}"] = pd.concat([pd.Series(self.log_death_rate[f"male_{i}"] - self.mort_constants[f"male_{i}"])], axis = 1)
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


def main():
    deaths_male = pd.read_csv("deaths_male.csv").set_index("year")
    exposure_male = pd.read_csv("exposure_male.csv").set_index("year")
    lc = mortModel(deaths_male, exposure_male)
    print(lc.centralized_matrix())
    print(lc.age_constants())
    print(lc.year_constants())


if __name__ == "__main__":
    main()
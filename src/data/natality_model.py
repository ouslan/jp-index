import pandas as pd
import numpy as np
from scipy import linalg


class natModel():
    def __init__(self, tfr):
        # Declare given dataframes
        self.tfr = tfr
    
    def nat_constants(self):
        # Aggregate the tfr by age groups
        aggregates = list(self.tfr.sum())
        averages = []
        # Start loop in range of 0 to the length of the index (number of age groups) of the original tfr DataFrame
        for i in range(0, len(self.tfr.columns.values)):
            # Calculate average tfr for age group i and append value to empty list
            averages.append(float(aggregates[i] / self.tfr.index.size))
        return averages
   

    def centralized_frame(self):
        # Create empty matrix with the years from the logged death rates matrix as the index
        centered_matrix = pd.DataFrame(index = self.tfr.index)
        tfr_t = self.tfr.copy().T.reset_index(drop=True).T
        for i in range(len(self.nat_constants())):
            # Calculate the centralized death rate at age group i by subtracting the mortality constant of group i from the values of the column of age group i and add to matrix
            centered_matrix[i] = pd.concat([pd.Series(tfr_t[i] - self.nat_constants()[i])], axis=1)
        return centered_matrix

    def nat_covar(self):
        cent_matrix = self.centralized_frame().to_numpy()
        cent_matrix_T = self.centralized_frame().T.to_numpy()
        covar_matrix = np.matmul(cent_matrix_T, cent_matrix)
        return covar_matrix
    
    def eigenvals(self):
        covar_matrix = self.nat_covar()
        w, v = linalg.eigh(covar_matrix)
        return pd.Series(w)
    
    def eigenvect(self):
        covar_matrix = self.nat_covar()
        w, v = linalg.eigh(covar_matrix)
        return pd.DataFrame(v)


def main():
    nat_data = pd.read_csv("tfr_data.csv").set_index("year")
    nat_model = natModel(nat_data)
    print(nat_model.eigenvals())
    print(nat_model.eigenvect())

    
if __name__ == "__main__":
    main()

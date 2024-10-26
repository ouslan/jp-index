import pandas as pd
from scipy.stats import truncnorm
from tqdm import tqdm

def main():
    # Male year constants simulation
    std_dev_m = 0.2996471
    std_dev_f = 0.3225237
    upper_normal = .8
    lower_normal = -0.3236444
    simulation_n = 100000


    df = pd.read_csv("kt_p.csv")
    df2 = pd.DataFrame()
    df3 = pd.DataFrame()
    count = 0
    for i in df["kt_m_p"]:
        sim_list = []
        for _ in tqdm(range(simulation_n)):
            n = truncnorm.rvs((lower_normal - i)/std_dev_m, (upper_normal - i)/std_dev_m, loc=i, scale=std_dev_m, size=1)
            sim_list.append(n.item())
        df2[f"{df["year"].iloc[count]}"] = pd.Series(sim_list)
        print(f"Year {df["year"].iloc[count]} done")
        count += 1
    for column in df2:
        perc_1 = float(df2[f"{column}"].quantile(0.01))
        perc_3 = float(df2[f"{column}"].quantile(0.0275))
        perc_5 = float(df2[f"{column}"].quantile(0.05))
        perc_50 = float(df2[f"{column}"].quantile(0.5))
        perc_95 = float(df2[f"{column}"].quantile(0.95))
        perc_97 = float(df2[f"{column}"].quantile(0.975))
        perc_99 = float(df2[f"{column}"].quantile(0.99))
        percentiles = pd.Series([perc_1, perc_3, perc_5, perc_50, perc_95, perc_97, perc_99])
        df3[f"{column}"] = percentiles
    df3.T.to_csv("mc_simulation_output.csv")


if __name__ == "__main__":
    main()

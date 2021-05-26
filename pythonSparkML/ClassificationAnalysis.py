import matplotlib.pyplot as plt
import scipy.stats as stats

def main():
    decision_tree_values = [0.0540541, 0.0384615, 0.0333333, 0.12, 0.0512821, 0.0689655, 0.0740741, 0.0344828,
                            0.0277778, 0.0588235, 0.107143, 0.0588235, 0.0333333, 0.0740741, 0.103448, 0.0740741,
                            0.0714286, 0.0588235, 0.037037]
    logistic_regression_values = [0.309524, 0.377778, 0.362069, 0.333333, 0.0612245, 0.125, 0.0666667, 0.208333,
                                  0.142857, 0.333333, 0.326531, 0.4, 0.27907, 0.307692, 0.265306, 0.319149,
                                  0.214286, 0.403846, 0.2]

    print("Data length:" + str(len(decision_tree_values)))  # plt.hist(decision_tree_values)
    print("Data length:" + str(len(logistic_regression_values)))  # plt.hist(decision_tree_values)
    plt.scatter(range(len(decision_tree_values)),decision_tree_values)
    plt.title("Decision tree error")
    plt.show()

    print(stats.mannwhitneyu(decision_tree_values, logistic_regression_values))
    # Se rechaza la hipotesis nula de MANN WHITNEY, uno de los algoritmos es mejor que otro

if __name__ == "__main__":
    main()
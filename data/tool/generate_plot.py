#from get_all_spectrum import get_all_spectrum
import pandas as pd
import os
import matplotlib.pyplot as plt
import seaborn as sns


#this is have error and i fix it 
def generate_plot_for_date_and_spectrum(date_and_spectrum : dict,
                                        path_destination : str) -> None:
    
    all_spectrum = get_all_spectrum(date_and_spectrum)
    
    result = {}
    for day , spectrum in date_and_spectrum.items():
        result[day] = len(all_spectrum) - len(spectrum)
    
    plot_with_number(result)



def plot_with_number(data : dict,
         path_destination : str) -> None:       

    plt.figure(figsize = (30 , 10))
    plots = sns.barplot(x = list(data.keys()), y = list(data.values()), color='black')
    plots.set_xticklabels(plots.get_xticklabels(),rotation = 90)
    for bar in plots.patches:
        plots.annotate(format(bar.get_height(), '.0f'),
                        (bar.get_x() + bar.get_width() / 2,
                            bar.get_height()), ha='center', va='center',
                        size=15, xytext=(0, 8),
                        textcoords='offset points')
    
    plt.savefig(path_destination)
        


def bar_plot(data : dict,
             path_destination : str ) -> None:
    x_values = list(data.keys())
    y_values = list(data.values())

    plt.plot(x_values, y_values)
    plt.savefig(path_destination)
    plt.close()


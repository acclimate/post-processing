import argparse
import os
from string import ascii_lowercase

import holoviews as hv
import numpy as np
import panel as pn
from bokeh.io import export_png
from bokeh.models import Div
from selenium import webdriver

from acclimate import dashboard_functions
from acclimate import definitions
from acclimate import helpers

# define default renderers
hv.extension('bokeh', "matplotlib")
import math

golden = (1 + math.sqrt(5)) / 2


# some plotting fucntions, to be moved after dev

def cap_elasticity_data(data, sector, variable1, variable2, variable1_cap=0):
    variable1_raw = data.select(sector=sector)[variable1]
    variable2_raw = data.select(sector=sector)[variable2]
    variable1_capped = np.where(np.abs(variable1_raw) > variable1_cap, variable1_raw, np.nan)
    variable2_capped = np.where(np.abs(variable1_raw) > variable1_cap, variable2_raw, np.nan)
    variable1_remainder = np.where(np.abs(variable1_raw) <= variable1_cap, variable1_raw, np.nan)
    variable2_remainder = np.where(np.abs(variable1_raw) <= variable1_cap, variable2_raw, np.nan)
    variable2_remainder = np.where(np.abs(variable2_remainder) <= 1.5, variable2_remainder, np.nan)
    return variable1_capped, variable2_capped, variable1_remainder, variable2_remainder


def point_plot_no_agents(data, variable1_label, variable2_label, cap=1):
    filtered = hv.HexTiles((data[0], data[1])).opts(cmap='fire', colorbar=True, width=500, xlabel=variable1_label,
                                                    ylabel=variable2_label, tools=['hover'], logz=True,
                                                    clim_percentile=False, ylim=(-1.5, 1.5))
    unfiltered = hv.HexTiles((data[2], data[3])).opts(cmap='fire', fill_alpha=0.25, line_alpha=0.25, alpha=0.25,
                                                      colorbar=False, width=500, xlabel=variable1_label,
                                                      ylabel=variable2_label, logz=True, clim_percentile=False,
                                                      ylim=(-1.5, 1.5))
    zero_line = hv.HLine(0).opts(color="Black", line_dash="dashed")
    return unfiltered * filtered * zero_line


def square_2dhist(x, y, x_label="x", ylabel="y"):
    z, a, b = np.histogram2d(x, y, range=[[-0.001, 0.001], [-1.5, 1.5]], bins=100)
    z = np.transpose(z)  # weird x,y interpretation of np.histogram2d makes transposing frequncy matrix necessary
    return hv.Image((a, b, z), [x_label, ylabel], 'Count')


def square_2dhist_density(x, y, x_label="x", ylabel="y", xrange=[-0.001, 0.001], bins=100):
    z, a, b = np.histogram2d(x, y, range=[xrange, [-1.5, 1.5]], bins=bins, density=True)
    z = np.transpose(z)  # weird x,y interpretation of np.histogram2d makes transposing frequncy matrix necessary
    return hv.Image((a, b, z), [x_label, ylabel], 'Density')


def point_plot_no_agents_squares(data, variable1_label, variable2_label, cap=1):
    filtered = square_2dhist(data[0], data[1]).opts(cmap='fire_r', colorbar=True, width=500, tools=['hover'],
                                                    clim_percentile=False, ylim=(-1.5, 1.5), logz=True, clim=(1, 1000))
    unfiltered = square_2dhist(data[2], data[3])[-cap:cap, :].opts(cmap='fire_r', alpha=1, colorbar=False, width=500,
                                                                   xlabel=variable1_label, ylabel=variable2_label,
                                                                   clim_percentile=False, ylim=(-1.5, 1.5), logz=True,
                                                                   clim=(1, 1000))
    zero_line = hv.HLine(0).opts(color="Black", line_dash="dashed")
    return unfiltered * filtered * zero_line


def point_plot_no_agents_squares_stripes(data, variable1_label, variable2_label, cap=1):
    windowsize = 0.00005
    window = (-0.001, 0.001)
    windowstarts = np.arange(window[0], window[1] + windowsize, windowsize)
    filtered = square_2dhist_density(data[0], data[1], xrange=[windowstarts[0], windowstarts[0] + windowsize],
                                     bins=[2, 50]).opts(cmap='fire_r', colorbar=True, width=500, tools=['hover'],
                                                        clim_percentile=False, ylim=(-1.5, 1.5), logz=True,
                                                        clim=(100, 100000))
    for i_windowstarts in windowstarts[1:-1]:
        stripe = square_2dhist_density(data[0], data[1], xrange=[i_windowstarts, i_windowstarts + windowsize],
                                       bins=[2, 50]).opts(cmap='fire_r', colorbar=False, width=500, tools=['hover'],
                                                          clim_percentile=False, ylim=(-1.5, 1.5), logz=True,
                                                          clim=(100, 100000))[
                 i_windowstarts + windowsize / 100:(i_windowstarts + windowsize * 2), :]
        filtered = filtered * stripe
    zero_line = hv.HLine(0).opts(color="Black", line_dash="dashed")
    return filtered * zero_line


# define regions and variables of interest
regions = ["CHN", "EU28", "USA", "CHN"]

variables = ["used_flow_elasticity"]

sectors = [0, 1, 2]

parser = argparse.ArgumentParser(description="Process acclimate output for easy and fast plotting.")
parser.add_argument(
    "--rootdir"
    , required=True,
    type=str,
    help="Path to the root directory, where subdata is in subfolders"
)
args = parser.parse_args()

driver = webdriver.Firefox(firefox_binary='/home/quante/miniconda3/bin/FirefoxApp/firefox',
                           executable_path='/home/quante/miniconda3/bin/geckodriver')

filename = "baseline_relative_consumer_basket_storage_"

for i in os.walk(args.rootdir):
    if os.path.normpath(i[0]) == os.path.normpath(args.rootdir):
        continue
    datadir = i[0]
    print(datadir)
    workdir = datadir

    r = 0
    for i_region in regions:
        region_data = helpers.load_region_basket_data(datadir, filename, i_region, elasticities=True)
        print(region_data.vdims)
        selected_region_data = pn.GridSpec(sizing_mode='stretch_both', mode='override')
        index = 0
        v = 0
        for i_variable in variables:
            s = 0
            for i_sector in sectors:
                letter = ascii_lowercase[index]
                explorer = dashboard_functions.get_region_boxplot(region_data, i_region, variable=i_variable,
                                                                  sector=i_sector, tools=False, baskets=True)
                explorer.opts(
                    hv.opts.BoxWhisker(xlabel="Income quintile", ylabel="Instant price elasticity", ylim=(-1.5, 1.5)))
                div = Div(text=letter, render_as_text=True)
                col = pn.Column(div, explorer, height=150, sizing_mode='stretch_width')
                selected_region_data[r + 1, s + v] = col
                s += 1
                index += 1
            v += 1
            export_png(selected_region_data.get_root(), filename=os.path.join(workdir, i_region + "_boxplot.png"),
                       webdriver=driver)
        r += 1

    r = 0
    for i_region in regions:
        region_data = helpers.load_region_basket_data(datadir, filename, i_region, elasticities=True)
        selected_region_data = pn.GridSpec(sizing_mode='stretch_both', mode='override')
        index = 0
        s = 0
        max_price_change_cap = np.min(region_data.select(sector=i_sector)["max_price_change_cap"])
        # only correct for basket data: using max price change cap based on "other" i.e. luxury goods
        for i_sector in sectors:
            letter = ascii_lowercase[index]
            filtered_data = cap_elasticity_data(region_data, i_sector, "used_flow_elasticity_price_change",
                                                "used_flow_elasticity", variable1_cap=0)
            plot = point_plot_no_agents_squares(filtered_data, variable1_label="Price change",
                                                variable2_label="Instant price elasticity", cap=0).opts(
                toolbar=None)  # TODO: decide cap/no cap
            plot.opts(title=i_region + ": " + definitions.consumption_baskets_index_name[i_sector])
            div = Div(text=letter, render_as_text=True)
            col = pn.Column(div, plot)
            selected_region_data[r + 1, s + v] = col
            s += 1
            index += 1
        v += 1
        export_png(selected_region_data.get_root(),
                   filename=os.path.join(workdir, i_region + "_elasticity_price_change_unfiltered.png"),
                   webdriver=driver)
        r += 1

        for i_region in regions:
            region_data = helpers.load_region_basket_data(datadir, filename, i_region, elasticities=True)
            selected_region_data = pn.GridSpec(sizing_mode='stretch_both', mode='override')
            index = 0
            s = 0
            max_price_change_cap = np.min(region_data.select(sector=i_sector)["max_price_change_cap"])
            # only correct for basket data: using max price change cap based on "other" i.e. luxury goods
            for i_sector in sectors:
                letter = ascii_lowercase[index]
                filtered_data = cap_elasticity_data(region_data, i_sector, "used_flow_elasticity_price_change",
                                                    "used_flow_elasticity", variable1_cap=max_price_change_cap)
                plot = point_plot_no_agents_squares(filtered_data, variable1_label="Price change",
                                                    variable2_label="Instant price elasticity",
                                                    cap=max_price_change_cap).opts(
                    toolbar=None)  # TODO: decide cap/no cap
                plot.opts(title=i_region + ": " + definitions.consumption_baskets_index_name[i_sector])
                div = Div(text=letter, render_as_text=True)
                col = pn.Column(div, plot)
                selected_region_data[r + 1, s + v] = col
                s += 1
                index += 1
            v += 1
            export_png(selected_region_data.get_root(),
                       filename=os.path.join(workdir, i_region + "_elasticity_price_change_filtered.png"),
                       webdriver=driver)
            r += 1

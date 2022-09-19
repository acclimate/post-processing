#!/usr/bin/python3 -W ignore

import gzip
import math
import pickle
from functools import partial

import matplotlib
import matplotlib.pyplot as plt
import numpy as np
import pyproj
from matplotlib.collections import PatchCollection
from matplotlib.colors import LinearSegmentedColormap, Normalize
from matplotlib.patches import PathPatch, Wedge
from matplotlib.path import Path
from mpl_toolkits.axes_grid1 import make_axes_locatable
from shapely.geometry import Point
from shapely.ops import transform


def create_colormap(name, colors, alphas=None, xs=None):
    def get_rgb1(c, alpha=1):
        return tuple(list(matplotlib.colors.hex2color(c)) + [alpha])

    if str(matplotlib.__version__).startswith("2"):
        get_rgb = matplotlib.colors.to_rgba
    else:
        get_rgb = get_rgb1
    if alphas is None:
        colors = [get_rgb(c) for c in colors]
    else:
        colors = [get_rgb(c, alpha=a) for c, a in zip(colors, alphas)]
    if xs is None:
        xs = np.linspace(0, 1, len(colors))
    res = LinearSegmentedColormap(
        name,
        {
            channel: tuple(
                (x, float(c[channel_id]), float(c[channel_id]))
                for c, x in zip(colors, xs)
            )
            for channel_id, channel in enumerate(["red", "green", "blue", "alpha"])
        },
        N=2048,
    )
    res.set_under(colors[0])
    res.set_over(colors[-1])
    return res


def make_map(
        patchespickle_file,
        map_regions=None,
        map_data=None,
        centroids_regions=None,
        centroids_data=None,
        centroids_data_unit=None,
        centroids_annotate=None,
        cm=None,
        symmetric_cmap=False,
        outfile=None,
        extend_c='both',
        numbering=None,
        y_label=None,
        rasterize=True,
        min_lon=-156,
        max_lon=170,
        min_lat=-58,
        max_lat=89,
        valid_ec='black',
        inv_fc='lightgrey',
        inv_ec='black',
        map_v_limits=None,
        centroids_v_limits=None,
):
    if map_data is None and centroids_data is None:
        raise ValueError("Must pass at least one of map_data and centroids_data to plot.")

    if centroids_annotate is None:
        centroids_annotate = []

    patchespickle = pickle.load(gzip.GzipFile(patchespickle_file, "rb"))
    patches = patchespickle["patches"]
    centroids = patchespickle["centroids"]
    projection_name = patchespickle["projection"]

    if cm is None:
        cm = create_colormap("custom", ["red", "white", "blue"], xs=[0, 0.5, 1])

    def my_transform(scale, t, trans, x, y):
        p = trans(x, y)
        return (p[0] * scale + t[0], p[1] * scale + t[1])

    def get_projection(to, scale=1, translate=(0, 0)):
        return partial(
            my_transform,
            scale,
            translate,
            partial(
                pyproj.transform,
                pyproj.Proj("+proj=lonlat +datum=WGS84 +no_defs"),
                pyproj.Proj(f"+proj={to} +datum=WGS84 +no_defs"),
            ),
        )

    projection = get_projection(projection_name)

    minx = transform(projection, Point(min_lon, 0)).x
    maxx = transform(projection, Point(max_lon, 0)).x
    miny = transform(projection, Point(0, min_lat)).y
    maxy = transform(projection, Point(0, max_lat)).y

    fig, (ax, cax) = plt.subplots(
        ncols=2,
        gridspec_kw={'width_ratios': [.975, .025]},
        figsize=(14, 13 / 2)
    )

    ax.set_aspect(1)
    ax.axis("off")

    ax.set_xlim((minx, maxx))
    ax.set_ylim((miny, maxy))

    validpatches = {}
    validpatches_data = {}
    invpatches = {}
    if map_data is not None:
        if map_v_limits is None:
            map_vmin = np.min(map_data)
            map_vmax = np.max(map_data)
        else:
            (map_vmin, map_vmax) = map_v_limits
        if symmetric_cmap and np.sign(map_vmin) != np.sign(map_vmax):
            v = max(abs(map_vmin), abs(map_vmax))
            (map_vmin, map_vmax) = (np.sign(map_vmin) * v, np.sign(map_vmax) * v)
        norm_color = Normalize(vmin=map_vmin, vmax=map_vmax)
        for r, d in zip(map_regions, map_data):
            if r in patches:
                level, subregions, patch = patches[r]
                if math.isnan(d):
                    invpatches[r] = patch
                    print('NAN data for region {}'.format(r))
                else:
                    validpatches[r] = patch
                    validpatches_data[r] = d
            else:
                print(f"Region {r} not found in patches.")
        valid_collection = ax.add_collection(
            PatchCollection(
                list(validpatches.values()),
                edgecolors=valid_ec,
                facecolors="black",
                linewidths=.1,
                rasterized=rasterize,
                zorder=1
            )
        )
        valid_collection.set_facecolors(cm(norm_color([validpatches_data[k] for k in validpatches.keys()])))
        cbar = matplotlib.colorbar.ColorbarBase(
            cax,
            cmap=cm,
            norm=norm_color,
            orientation="vertical",
            spacing="proportional",
            extend=extend_c,
        )
        cbar.minorticks_on()

    for r, (level, subregions, patch) in patches.items():
        if len(subregions) == 1 and r not in validpatches and r not in invpatches:
            invpatches[r] = patch
            print('No data passed for region {}'.format(r))

    ax.add_collection(
        PatchCollection(
            list(invpatches.values()),
            hatch="///",
            facecolors=inv_fc,
            edgecolors=inv_ec,
            linewidths=.1,
            rasterized=rasterize,
            zorder=0
        )
    )

    if centroids_data is not None:
        if centroids_v_limits is None:
            centroids_vmin = np.min(centroids_data)
            centroids_vmax = np.max(centroids_data)
        else:
            (centroids_vmin, centroids_vmax) = centroids_v_limits

        def get_radius(_d):
            return .05 * _d / centroids_vmax * (abs(ax.get_ylim()[0]) + abs(ax.get_ylim()[1]))
        wedges = []
        for r, d in zip(centroids_regions, centroids_data):
            if r in centroids:
                wedges.append(Wedge(centroids[r], get_radius(d), 0, 360))
                if r in centroids_annotate:
                    ax.text(centroids[r].x, centroids[r].y, r, ha='center', va='center')
        # legend_wedges = []
        x_pos = ax.get_xlim()[0] + get_radius(centroids_vmin)
        y_pos = ax.get_ylim()[0] + get_radius(centroids_vmax)
        for d in np.linspace(centroids_vmin, centroids_vmax, 3):
            radius = get_radius(d)
            x_pos += 2 * radius
            wedges.append(Wedge((x_pos, y_pos), radius, 0, 360))
            text = f"{int(np.round(d, 0))}"
            if centroids_data_unit is not None:
                text += "{}".format(centroids_data_unit)
            ax.text(x_pos, y_pos, text, ha='center', va='center')
        ax.add_collection(
            PatchCollection(
                wedges,
                facecolors='lightgrey',
                edgecolors='black',
                linewidths=.1,
                rasterized=rasterize,
                zorder=1
            )
        )

    plt.tight_layout()

    if numbering is not None:
        ax.text(
            0.0, 1.0, numbering, transform=ax.transAxes, fontweight='bold'
        )

    if outfile is not None:
        fig.savefig(outfile, dpi=300)


# pathespickle files as well as the script to generate these can be found on the cluster at
# /p/projects/acclimate/data/plotting
def make_map_deprecated(
        patchespickle_file,
        regions,
        data,
        show_cbar=True,
        cm=None,
        outfile=None,
        ax=None,
        cax=None,
        extend_c="both",
        ignore_regions=None,
        invalid_edgecolor="lightgrey",
        invalid_facecolor="lightgrey",
        linewidth=0.1,
        norm_color=None,
        numbering=None,
        numbering_fontsize=10,
        rasterize=True,
        title=None,
        title_fontsize=10,
        valid_edgecolor="black",
        y_label=None,
        y_label_fontsize=10,
        y_ticks=None,
        y_tick_labels=None,
        y_ticks_fontsize=8,
        lims=None,
        v_limits=None
):
    if ignore_regions is None:
        ignore_regions = ["ATA"]
    if cm is None:
        cm = create_colormap("custom", ["red", "white", "blue"], xs=[0, 0.5, 1])

    patchespickle = pickle.load(gzip.GzipFile(patchespickle_file, "rb"))
    patches = patchespickle["patches"]
    projection_name = patchespickle["projection"]

    if y_ticks is None:
        vmin = np.min(data)
        vmax = np.max(data)
    else:
        vmin = y_ticks[0]
        vmax = y_ticks[-1]

    if v_limits is not None:
        (vmin, vmax) = v_limits

    if norm_color is None:
        norm_color = Normalize(vmin=vmin, vmax=vmax)

    def EmptyPatch():
        return PathPatch(Path([(0, 0)], [Path.MOVETO]))

    def my_transform(scale, t, trans, x, y):
        p = trans(x, y)
        return (p[0] * scale + t[0], p[1] * scale + t[1])

    def get_projection(to, scale=1, translate=(0, 0)):
        return partial(
            my_transform,
            scale,
            translate,
            partial(
                pyproj.transform,
                pyproj.Proj("+proj=lonlat +datum=WGS84 +no_defs"),
                pyproj.Proj(f"+proj={to} +datum=WGS84 +no_defs"),
            ),
        )

    projection = get_projection(projection_name)
    if lims is None:
        miny, maxy, minx, maxx = -58, 89, -156, 170
    else:
        miny, maxy, minx, maxx = lims
    minx = transform(projection, Point(minx, 0)).x
    maxx = transform(projection, Point(maxx, 0)).x
    miny = transform(projection, Point(0, miny)).y
    maxy = transform(projection, Point(0, maxy)).y

    width_ratios = [1]  # , 0.005, 0.03] TODO
    if isinstance(outfile, str):
        # figure widths: 2.25 inches (1 column) or 4.75 inches (2 columns)
        fig = plt.figure(figsize=(4.75, 3))
        gs_base = plt.GridSpec(1, len(width_ratios), width_ratios=width_ratios, wspace=0)
    elif ax is None:
        fig = outfile.get_gridspec().figure
        gs_base = outfile.subgridspec(1, len(width_ratios), width_ratios=width_ratios, wspace=0)
    if ax is None:
        ax = fig.add_subplot(gs_base[:, 0])
    ax.set_xlim(minx, maxx)
    ax.set_ylim(miny, maxy)
    ax.set_aspect(1)
    ax.set_xticks([])
    ax.set_yticks([])
    ax.axis("off")
    if title is not None:
        ax.set_title(title, fontsize=title_fontsize)

    invpatches = []
    validpatches = []

    for r, d in zip(regions, data):
        if r in patches:
            level, subregions, patch = patches[r]
            if math.isnan(d):
                validpatches.append(EmptyPatch())
                invpatches.append(patch)
                print('NAN data for region {}'.format(r))
            elif r in ignore_regions:
                validpatches.append(EmptyPatch())
                invpatches.append(patch)
                print('Ignore region {}'.format(r))
            else:
                validpatches.append(patch)
        else:
            validpatches.append(EmptyPatch())

    ax.add_collection(
        PatchCollection(
            invpatches,
            hatch="///",
            facecolors=invalid_facecolor,
            edgecolors=invalid_edgecolor,
            linewidths=linewidth,
            rasterized=rasterize,
        )
    )

    if numbering is not None:
        ax.text(
            0.0, 1.0, numbering, fontsize=numbering_fontsize, transform=ax.transAxes, fontweight='bold'
        )

    region_collection = ax.add_collection(
        PatchCollection(
            validpatches,
            edgecolors=valid_edgecolor,
            facecolors="black",
            linewidths=linewidth,
            rasterized=rasterize,
        )
    )

    if show_cbar:
        if cax is None:
            divider = make_axes_locatable(ax)
            cax = divider.append_axes("right", size="5%", pad=0.1)
        cbar = matplotlib.colorbar.ColorbarBase(
            cax,
            cmap=cm,
            norm=norm_color,
            ticks=y_ticks,
            orientation="vertical",
            spacing="proportional",
            extend=extend_c,
        )
        cbar.minorticks_on()
        if y_tick_labels is not None:
            cbar.ax.set_yticklabels(y_tick_labels)
        if y_label is not None:
            cax.set_ylabel(y_label, fontsize=y_label_fontsize)
        cax.tick_params(axis="y", labelsize=y_ticks_fontsize)

    # region_collection.set_facecolors('r')
    region_collection.set_facecolors(cm(norm_color(data)))

    if isinstance(outfile, str):
        # plt.subplots_adjust(bottom=0.02, top=0.98, left=0.05, right=0.9)
        plt.tight_layout()
        fig.savefig(outfile, dpi=300)

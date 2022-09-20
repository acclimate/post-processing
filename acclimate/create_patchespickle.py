#!/usr/bin/python3 -W ignore
# -*- coding: utf-8 -*-

import argparse
import gzip
import pickle
import sys
import numpy as np

import fiona
from descartes import PolygonPatch
from pyproj import Transformer
from shapely.geometry import shape
from shapely.ops import transform, unary_union
from tqdm import tqdm
from multiprocessing import Pool

iso3_to_iso2 = {
    "ABW": "AW",
    "AFG": "AF",
    "AGO": "AO",
    "AIA": "AI",
    "ALA": "AX",
    "ALB": "AL",
    "AND": "AD",
    "ANT": "AN",
    "ARE": "AE",
    "ARG": "AR",
    "ARM": "AM",
    "ASM": "AS",
    "ATA": "AQ",
    "ATF": "TF",
    "ATG": "AG",
    "AUS": "AU",
    "AUT": "AT",
    "AZE": "AZ",
    "BDI": "BI",
    "BEL": "BE",
    "BEN": "BJ",
    "BFA": "BF",
    "BGD": "BD",
    "BGR": "BG",
    "BHR": "BH",
    "BHS": "BS",
    "BIH": "BA",
    "BLM": "BL",
    "BLR": "BY",
    "BLZ": "BZ",
    "BMU": "BM",
    "BOL": "BO",
    "BRA": "BR",
    "BRB": "BB",
    "BRN": "BN",
    "BTN": "BT",
    "BWA": "BW",
    "CAF": "CF",
    "CAN": "CA",
    "CHE": "CH",
    "CHL": "CL",
    "CHN": "CN",
    "CIV": "CI",
    "CMR": "CM",
    "COD": "CD",
    "COG": "CG",
    "COK": "CK",
    "COL": "CO",
    "COM": "KM",
    "CPV": "CV",
    "CRI": "CR",
    "CUB": "CU",
    "CUW": "CW",
    "CYM": "KY",
    "CYP": "CY",
    "CZE": "CZ",
    "DEU": "DE",
    "DJI": "DJ",
    "DMA": "DM",
    "DNK": "DK",
    "DOM": "DO",
    "DZA": "DZ",
    "ECU": "EC",
    "EGY": "EG",
    "ERI": "ER",
    "ESH": "EH",
    "ESP": "ES",
    "EST": "EE",
    "ETH": "ET",
    "FIN": "FI",
    "FJI": "FJ",
    "FLK": "FK",
    "FRA": "FR",
    "FRO": "FO",
    "FSM": "FM",
    "GAB": "GA",
    "GBR": "GB",
    "GEO": "GE",
    "GGY": "GG",
    "GHA": "GH",
    "GIB": "GI",
    "GIN": "GN",
    "GMB": "GM",
    "GNB": "GW",
    "GNQ": "GQ",
    "GRC": "GR",
    "GRD": "GD",
    "GRL": "GL",
    "GTM": "GT",
    "GUM": "GU",
    "GUY": "GY",
    "HKG": "HK",
    "HMD": "HM",
    "HND": "HN",
    "HRV": "HR",
    "HTI": "HT",
    "HUN": "HU",
    "IDN": "ID",
    "IMN": "IM",
    "IND": "IN",
    "IOT": "IO",
    "IRL": "IE",
    "IRN": "IR",
    "IRQ": "IQ",
    "ISL": "IS",
    "ISR": "IL",
    "ITA": "IT",
    "JAM": "JM",
    "JEY": "JE",
    "JOR": "JO",
    "JPN": "JP",
    "KAZ": "KZ",
    "KEN": "KE",
    "KGZ": "KG",
    "KHM": "KH",
    "KIR": "KI",
    "KNA": "KN",
    "KOR": "KR",
    "KWT": "KW",
    "LAO": "LA",
    "LBN": "LB",
    "LBR": "LR",
    "LBY": "LY",
    "LCA": "LC",
    "LIE": "LI",
    "LKA": "LK",
    "LSO": "LS",
    "LTU": "LT",
    "LUX": "LU",
    "LVA": "LV",
    "MAC": "MO",
    "MAF": "MF",
    "MAR": "MA",
    "MCO": "MC",
    "MDA": "MD",
    "MDG": "MG",
    "MDV": "MV",
    "MEX": "MX",
    "MHL": "MH",
    "MKD": "MK",
    "MLI": "ML",
    "MLT": "MT",
    "MMR": "MM",
    "MNE": "ME",
    "MNG": "MN",
    "MNP": "MP",
    "MOZ": "MZ",
    "MRT": "MR",
    "MSR": "MS",
    "MUS": "MU",
    "MWI": "MW",
    "MYS": "MY",
    "NAM": "NA",
    "NCL": "NC",
    "NER": "NE",
    "NFK": "NF",
    "NGA": "NG",
    "NIC": "NI",
    "NIU": "NU",
    "NLD": "NL",
    "NPL": "NP",
    "NRU": "NR",
    "NZL": "NZ",
    "OMN": "OM",
    "PAK": "PK",
    "PAN": "PA",
    "PCN": "PN",
    "PER": "PE",
    "PHL": "PH",
    "PLW": "PW",
    "PNG": "PG",
    "POL": "PL",
    "PRI": "PR",
    "PRK": "KP",
    "PRT": "PT",
    "PRY": "PY",
    "PSE": "PS",
    "PYF": "PF",
    "QAT": "QA",
    "ROU": "RO",
    "RUS": "RU",
    "RWA": "RW",
    "SAU": "SA",
    "SDN": "SD",
    "SEN": "SN",
    "SGP": "SG",
    "SGS": "GS",
    "SHN": "SH",
    "SLB": "SB",
    "SLE": "SL",
    "SLV": "SV",
    "SMR": "SM",
    "SOM": "SO",
    "SPM": "PM",
    "SRB": "RS",
    "SSD": "SS",
    "STP": "ST",
    "SUR": "SR",
    "SVK": "SK",
    "SVN": "SI",
    "SWE": "SE",
    "SWZ": "SZ",
    "SXM": "SX",
    "SYC": "SC",
    "SYR": "SY",
    "TCA": "TC",
    "TCD": "TD",
    "TGO": "TG",
    "THA": "TH",
    "TJK": "TJ",
    "TKM": "TM",
    "TLS": "TL",
    "TON": "TO",
    "TTO": "TT",
    "TUN": "TN",
    "TUR": "TR",
    "TUV": "TV",
    "TWN": "TW",
    "TZA": "TZ",
    "UGA": "UG",
    "UKR": "UA",
    "UMI": "UM",
    "URY": "UY",
    "USA": "US",
    "UZB": "UZ",
    "VAT": "VA",
    "VCT": "VC",
    "VEN": "VE",
    "VGB": "VG",
    "VIR": "VI",
    "VNM": "VN",
    "VUT": "VU",
    "WLF": "WF",
    "WSM": "WS",
    "YEM": "YE",
    "ZAF": "ZA",
    "ZMB": "ZM",
    "ZWE": "ZW",
    # included by hand:
    "BES": "BQ",  # Bonaire, Saint Eustatius and Saba
    "BVT": "BV",  # Bouvet Island
    "CCK": "CC",  # Cocos Islands
    "CXR": "CX",  # Christmas Island
    "GLP": "GP",  # Guadeloupe
    "GUF": "GF",  # French Guiana
    "MTQ": "MQ",  # Martinique
    "MYT": "YT",  # Mayotte
    "NOR": "NO",  # Norway
    "REU": "RE",  # Reunion
    "SJM": "SJ",  # Svalbard and Jan Mayen
    "SP-": "SP",  # Spratly islands
    "XSP": "SP",  # Spratly islands
    "TKL": "TK",  # Tokelau
    # do not have an iso2:
    "PIS": "XP",  # Paracel Islands
    "XPI": "XP",  # Paracel Islands
    "XAD": "XA",  # Akrotiri and Dhekelia
    "XCL": "XC",  # Clipperton Island
    "XKO": "XK",  # Kosovo
    "XNC": "XN",  # Northern Cyprus
}

remap_regions = {
    'CHN.HKG': 'HKG',
    'CHN.MAC': 'MAC',
}

world_regions = {
    'AFR': [
        'DZA', 'AGO', 'CPV', 'TZA', 'BWA', 'DJI', 'GIN', 'SYC', 'MAR', 'ZAF', 'BEN', 'CMR', 'LSO', 'TCD', 'MOZ', 'GNQ',
        'COG', 'ETH', 'MDG', 'RWA', 'ZMB', 'CAF', 'SOM', 'ERI', 'GAB', 'STP', 'EGY', 'NAM', 'GHA', 'LBR', 'LBY', 'BFA',
        'MRT', 'NGA', 'MWI', 'UGA', 'BDI', 'MUS', 'NER', 'SEN', 'GMB', 'ZWE', 'KEN', 'TUN', 'SWZ', 'CIV', 'TGO', 'MLI',
        'SLE', 'COD', 'SDN', 'SSD'
    ],
    'ASI': [
        'BGD', 'QAT', 'PAK', 'VNM', 'THA', 'NPL', 'YEM', 'PHL', 'SYR', 'MAC', 'GEO', 'TJK', 'PSE', 'IND', 'MDV', 'MMR',
        'RUS', 'KOR', 'IRQ', 'IRN', 'ARE', 'BHR', 'ARM', 'PNG', 'JOR', 'MYS', 'PRK', 'KHM', 'HKG', 'SAU', 'LBN', 'CHN',
        'KAZ', 'LKA', 'TKM', 'MNG', 'AFG', 'BTN', 'ISR', 'IDN', 'LAO', 'TUR', 'OMN', 'BRN', 'TWN', 'AZE', 'SGP', 'UZB',
        'KWT', 'JPN', 'KGZ'
    ],
    'EUR': [
        'BGR', 'FIN', 'ROU', 'BEL', 'GBR', 'HUN', 'BLR', 'GRC', 'AND', 'NOR', 'SMR', 'MDA', 'SRB', 'LTU', 'SWE',
        'AUT', 'ALB', 'MKD', 'UKR', 'CHE', 'LIE', 'PRT', 'SVN', 'SVK', 'HRV', 'DEU', 'NLD', 'MNE', 'LVA', 'IRL', 'CZE',
        'LUX', 'ISL', 'FRA', 'DNK', 'ITA', 'CYP', 'BIH', 'POL', 'EST', 'ESP', 'MLT', 'MCO'
    ],
    'LAM': [
        'NIC', 'GUY', 'CRI', 'TTO', 'PAN', 'BLZ', 'VGB', 'HND', 'DOM', 'PER', 'COL', 'VEN', 'MEX', 'ABW', 'ARG', 'BHS',
        'BOL', 'PRY', 'CHL', 'JAM', 'URY', 'HTI', 'ATG', 'SUR', 'ECU', 'GTM', 'CUB', 'BRB', 'BRA', 'CYM', 'SLV'
    ],
    'NMA': [
        'GRL', 'CAN', 'BMU', 'USA'
    ],
    'OCE': [
        'FJI', 'VUT', 'AUS', 'WSM', 'NZL', 'NCL', 'PYF'
    ],
    'ADB': [
        'BGD', 'PAK', 'VNM', 'THA', 'NPL', 'PHL', 'GEO', 'TJK', 'IND', 'MDV', 'MMR', 'KOR', 'ARE', 'ARM', 'PNG', 'MYS',
        'HKG', 'CHN', 'KAZ', 'TKM', 'MNG', 'AFG', 'BTN', 'IDN', 'LAO', 'TUR', 'TWN', 'AZE', 'UZB', 'JPN', 'KGZ'
    ],
    'EU28': [
        'BGR', 'FIN', 'ROU', 'BEL', 'GBR', 'HUN', 'GRC', 'LTU', 'SWE', 'AUT', 'PRT', 'SVN', 'SVK', 'HRV', 'DEU', 'NLD',
        'LVA', 'IRL', 'CZE', 'LUX', 'FRA', 'DNK', 'ITA', 'CYP', 'POL', 'EST', 'ESP', 'MLT'
    ],
    'EU27': [
        'BGR', 'FIN', 'ROU', 'BEL', 'HUN', 'GRC', 'LTU', 'SWE', 'AUT', 'PRT', 'SVN', 'SVK', 'HRV', 'DEU', 'NLD',
        'LVA', 'IRL', 'CZE', 'LUX', 'FRA', 'DNK', 'ITA', 'CYP', 'POL', 'EST', 'ESP', 'MLT'
    ],
    'OECD': [
        'AUS', 'AUT', 'BEL', 'CAN', 'CHE', 'CHL', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'GBR', 'GRC', 'HUN',
        'IRL', 'ISL', 'ISR', 'ITA', 'JPN', 'KOR', 'LUX', 'LVA', 'MEX', 'NLD', 'NOR', 'NZL', 'POL', 'PRT', 'SVK', 'SVN',
        'SWE', 'TUR', 'USA'
    ],
    'BRICS': [
        'BRA', 'CHN', 'IND', 'RUS', 'ZAF'
    ],
    'ANT': [
        'BES', 'CUW', 'SXM', 'ABW'
    ]
}


parser = argparse.ArgumentParser(description="")
parser.add_argument("patchespickle", type=str, help="")
parser.add_argument("--simplified", type=float, help="")
parser.add_argument("--shapefile", type=str, default="/home/robin/data/GADM/gadm_410-levels.gpkg", help="")
parser.add_argument("--proj", type=str, default="World_Robinson", help="")
parser.add_argument("--admin1_countries", type=str, default="CHN+USA", help="")
args = parser.parse_args()

admin1_countries = list(set(args.admin1_countries.split('+')) - {''})
num_levels = 1
if len(admin1_countries) > 0:
    num_levels = 2
tmp = {}
for level in tqdm(range(num_levels), desc="Level    "):
    for sr in tqdm(
        fiona.open(args.shapefile, layer=level),
        desc="Shapefile",
        leave=False,
    ):
        if sr['properties'][f"GID_{level}"] not in ['NA', '?']:
            ctry = sr['properties'][f"GID_0"]
            ctry = remap_regions.get(ctry, ctry)
            s = shape(sr["geometry"])
            if level == 0:
                tmp[ctry] = (0, [ctry], [s])
            if level == 1 and ctry in admin1_countries:
                hasc_1 = sr['properties']['HASC_1']
                if '.' in hasc_1:
                    tmp[ctry][1].append(hasc_1)
                    tmp[hasc_1] = (0, [hasc_1], [s])
                else:
                    ctry = sr['properties'][f"GID_1"]
                    ctry = remap_regions.get(ctry, ctry)
                    tmp[ctry] = (0, [ctry], [s])
        else:
            tqdm.write(f"Warning: NA GID_0:\n{sr['properties']}", file=sys.stderr)

for wr, wr_regions in world_regions.items():
    if np.all(np.isin(wr_regions, list(tmp.keys()))):
        tmp[wr] = (-1, [], [])
        for wr_region in wr_regions:
            tmp[wr][1].append(wr_region)
            tmp[wr][2].append(tmp[wr_region][2][0])
    else:
        print(f'Warning: Not all subregions for {wr} were found.')


def unary_union_(args_):
    return [args_[0], set(args_[1]), unary_union(args_[2])]


with Pool() as p:
    unions = [res for res in tqdm(p.imap(unary_union_, tmp.values()), desc='Union  ', total=len(tmp))]

projection = Transformer.from_crs("EPSG:4326", args.proj)


def get_patch(_shape):
    if args.simplified is not None:
        return PolygonPatch(
            transform(projection.transform, _shape.simplify(args.simplified))
        )
    else:
        return PolygonPatch(transform(projection.transform, _shape))


shapes_it = iter(tqdm(zip(tmp.keys(), unions), desc="Patches  "))
patches = {}
for k, v in shapes_it:
    patch = get_patch(v[2])
    patches[k] = (v[0], v[1], patch)

shapes_it = iter(tqdm(zip(tmp.keys(), unions), desc="Patches  "))
centroids = {}
for k, v in shapes_it:
    centroids[k] = transform(projection.transform, v[2].centroid)

bar = tqdm(desc="Pickle")
pickle.dump(
    {"projection": f"{args.proj}", "patches": patches, "centroids": centroids},
    gzip.GzipFile(args.patchespickle, "wb"),
    pickle.HIGHEST_PROTOCOL,
)
bar.update(1)
bar.close()

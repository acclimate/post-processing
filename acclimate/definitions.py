# TODO important definitions such as standard region sets
import numpy as np
from matplotlib.colors import LinearSegmentedColormap

WORLD_REGIONS = {
    "AFR": [
        "DZA",
        "AGO",
        "CPV",
        "TZA",
        "BWA",
        "DJI",
        "GIN",
        "SYC",
        "MAR",
        "ZAF",
        "BEN",
        "CMR",
        "LSO",
        "TCD",
        "MOZ",
        # "GNQ",
        "COG",
        "ETH",
        "MDG",
        "RWA",
        "ZMB",
        "CAF",
        "SOM",
        "ERI",
        "GAB",
        "STP",
        "EGY",
        "NAM",
        "GHA",
        "LBR",
        "LBY",
        "BFA",
        "MRT",
        "NGA",
        "MWI",
        "UGA",
        "BDI",
        "MUS",
        "NER",
        "SEN",
        "GMB",
        "ZWE",
        "KEN",
        "TUN",
        "SWZ",
        "CIV",
        "TGO",
        "MLI",
        "SLE",
        "COD",
        "SDN",
        # "SDS",
    ],
    "ASI": [
        "BGD",
        "QAT",
        "PAK",
        "VNM",
        "THA",
        "NPL",
        "YEM",
        "PHL",
        "SYR",
        "MAC",
        "GEO",
        "TJK",
        "PSE",
        "IND",
        "MDV",
        "MMR",
        "RUS",
        "KOR",
        "IRQ",
        "IRN",
        "ARE",
        "BHR",
        "ARM",
        "PNG",
        "JOR",
        "MYS",
        "PRK",
        "KHM",
        "HKG",
        "SAU",
        "LBN",
        "KAZ",
        "LKA",
        "TKM",
        "MNG",
        "AFG",
        "BTN",
        "ISR",
        "IDN",
        "LAO",
        "TUR",
        "OMN",
        "BRN",
        "TWN",
        "AZE",
        "SGP",
        "UZB",
        "KWT",
        "JPN",
        "KGZ",
        "CHN"
    ],
    "EUR": [
        "BGR",
        "FIN",
        "ROU",
        "BEL",
        "GBR",
        "HUN",
        "BLR",
        "GRC",
        "AND",
        "ANT",
        "NOR",
        "SMR",
        # "MDA",
        "SRB",
        "LTU",
        "SWE",
        "AUT",
        "ALB",
        "MKD",
        "UKR",
        "CHE",
        "LIE",
        "PRT",
        "SVN",
        "SVK",
        "HRV",
        "DEU",
        "NLD",
        "MNE",
        "LVA",
        "IRL",
        "CZE",
        "LUX",
        "ISL",
        "FRA",
        "DNK",
        "ITA",
        "CYP",
        "BIH",
        "POL",
        "EST",
        "ESP",
        "MLT",
        "MCO",
    ],
    "LAM": [
        "NIC",
        "GUY",
        "CRI",
        "TTO",
        "PAN",
        "BLZ",
        "VGB",
        "HND",
        "DOM",
        "PER",
        "COL",
        "VEN",
        "MEX",
        "ABW",
        "ARG",
        "BHS",
        "BOL",
        "PRY",
        "CHL",
        "JAM",
        "URY",
        "HTI",
        "ATG",
        "SUR",
        "ECU",
        "GTM",
        "CUB",
        "BRB",
        "BRA",
        "CYM",
        "SLV",
    ],
    "NAM": ["GRL", "CAN", "BMU", "USA"],
    "OCE": ["FJI", "VUT", "AUS", "WSM", "NZL", "NCL", "PYF"],
    "ADB": [
        "BGD",
        "PAK",
        "VNM",
        "THA",
        "NPL",
        "PHL",
        "GEO",
        "TJK",
        "IND",
        "MDV",
        "MMR",
        "KOR",
        "ARE",
        "ARM",
        "PNG",
        "MYS",
        "HKG",
        "CHN",
        "KAZ",
        "TKM",
        "MNG",
        "AFG",
        "BTN",
        "IDN",
        "LAO",
        "TUR",
        "TWN",
        "AZE",
        "UZB",
        "JPN",
        "KGZ",
    ],
    "EU28": [
        "BGR",
        "FIN",
        "ROU",
        "BEL",
        "GBR",
        "HUN",
        "GRC",
        "LTU",
        "SWE",
        "AUT",
        "PRT",
        "SVN",
        "SVK",
        "HRV",
        "DEU",
        "NLD",
        "LVA",
        "IRL",
        "CZE",
        "LUX",
        "FRA",
        "DNK",
        "ITA",
        "CYP",
        "POL",
        "EST",
        "ESP",
        "MLT",
    ],
    "EU27": [
        "BGR",
        "FIN",
        "ROU",
        "BEL",
        "HUN",
        "GRC",
        "LTU",
        "SWE",
        "AUT",
        "PRT",
        "SVN",
        "SVK",
        "HRV",
        "DEU",
        "NLD",
        "LVA",
        "IRL",
        "CZE",
        "LUX",
        "FRA",
        "DNK",
        "ITA",
        "CYP",
        "POL",
        "EST",
        "ESP",
        "MLT",
    ],
    "OECD": [
        "AUS",
        "AUT",
        "BEL",
        "CAN",
        "CHE",
        "CHL",
        "CZE",
        "DEU",
        "DNK",
        "ESP",
        "EST",
        "FIN",
        "FRA",
        "GBR",
        "GRC",
        "HUN",
        "IRL",
        "ISL",
        "ISR",
        "ITA",
        "JPN",
        "KOR",
        "LUX",
        "LVA",
        "MEX",
        "NLD",
        "NOR",
        "NZL",
        "POL",
        "PRT",
        "SVK",
        "SVN",
        "SWE",
        "TUR",
        "USA",
    ],
    "BRICS": ["BRA", "CHN", "IND", "RUS", "ZAF"],
    "CHN": [
        "CN.AH",
        "CN.BJ",
        "CN.CQ",
        "CN.FJ",
        "CN.GS",
        "CN.GD",
        "CN.GX",
        "CN.GZ",
        "CN.HA",
        "CN.HB",
        "CN.HL",
        "CN.HE",
        "CN.HU",
        "CN.HN",
        "CN.JS",
        "CN.JX",
        "CN.JL",
        "CN.LN",
        "CN.NM",
        "CN.NX",
        "CN.QH",
        "CN.SA",
        "CN.SD",
        "CN.SH",
        "CN.SX",
        "CN.SC",
        "CN.TJ",
        "CN.XJ",
        "CN.XZ",
        "CN.YN",
        "CN.ZJ",
    ],
    "USA": [
        "US.AL",
        "US.AK",
        "US.AZ",
        "US.AR",
        "US.CA",
        "US.CO",
        "US.CT",
        "US.DE",
        "US.DC",
        "US.FL",
        "US.GA",
        "US.HI",
        "US.ID",
        "US.IL",
        "US.IN",
        "US.IA",
        "US.KS",
        "US.KY",
        "US.LA",
        "US.ME",
        "US.MD",
        "US.MA",
        "US.MI",
        "US.MN",
        "US.MS",
        "US.MO",
        "US.MT",
        "US.NE",
        "US.NV",
        "US.NH",
        "US.NJ",
        "US.NM",
        "US.NY",
        "US.NC",
        "US.ND",
        "US.OH",
        "US.OK",
        "US.OR",
        "US.PA",
        "US.RI",
        "US.SC",
        "US.SD",
        "US.TN",
        "US.TX",
        "US.UT",
        "US.VT",
        "US.VA",
        "US.WA",
        "US.WV",
        "US.WI",
        "US.WY",
    ],
    "G20": [
        "CHN",
        "IND",
        "USA",
        "IDN",
        "BRA",
        "RUS",
        "JPN",
        "MEX",
        "DEU",
        "TUR",
        "FRA",
        "GBR",
        "ITA",
        "ZAF",
        "KOR",
        "ARG",
        "CAN",
        "SAU",
        "AUS"
    ],
    "G20_REST": [
        "IND",
        "IDN",
        "BRA",
        "RUS",
        "JPN",
        "MEX",
        "TUR",
        "ZAF",
        "KOR",
        "ARG",
        "CAN",
        "SAU",
        "AUS"
    ],
    "BRIS": [
        "IND",
        "BRA",
        "RUS",
        "ZAF",
    ]
    # N.B. since EU is 20th member of the G20, only 19 entries
}

# consumption baskets for consumer analysis
necessary = ["AGRI", "FOOD", "ELWA", "FISH", "EDHE"]
relevant = ["OILC", "TRAN", "WOOD", "COMM", "RETT", "TEXL", "METL", "MACH", "TREQ", "MANU", "CONS", "WHOT"]
other = ["GAST", "MINQ", "REXI", "RECY", "OTHE", "FINC", "HOUS", "ADMI", "REPA"]
consumption_baskets = {"necessary": necessary, "relevant": relevant, "other": other}

# sector names
# define maps to map numerical dimensions back to keys
consumer_names = ["1st", "2nd", "3rd", "4th", "5th"]
consumer_indizes = range(0, 5)

sector_names = ['AGRI',
                'FISH',
                'MINQ',
                'FOOD',
                'TEXL',
                'WOOD',
                'OILC',
                'METL',
                'MACH',
                'TREQ',
                'MANU',
                'RECY',
                'ELWA',
                'CONS',
                'REPA',
                'WHOT',
                'RETT',
                'GAST',
                'TRAN',
                'COMM',
                'FINC',
                'ADMI',
                'EDHE',
                'HOUS',
                'OTHE',
                'REXI']
sector_indizes = range(0, 26)

consumer_index_name_dict = dict(zip(consumer_indizes, consumer_names))
consumer_name_index_dict = dict(zip(consumer_names, consumer_indizes))


def consumer_map(consumer_index):
    return consumer_index_name_dict[consumer_index]


producing_sectors_index_name_dict = dict(zip(sector_indizes, sector_names))
producing_sectors_name_index_dict = dict(zip(sector_names, sector_indizes))


def producing_sector_map(sector_index):
    return producing_sectors_index_name_dict[sector_index]


# colors as in previous publications

def pik_color(tone, id=0):
    return {
        "orange": ["#fab792", "#f89667", "#f57744", "#f35a28", "#de5224", "#c94a20", "#b4421c"],
        "gray": ["#bec1c3", "#a0a4a7", "#83888b", "#686c70", "#55585b", "#434346", "#302f32"],
        "blue": ["#99dff9", "#66cef6", "#33bef3", "#00adef", "#008dc7", "#006e9e", "#004e75"],
        "green": ["#cce3b7", "#b3d698", "#9aca7c", "#81be63", "#6a9c51", "#537a3f", "#3d582d"]
    }[tone][3 - id]


pik_cols = {'blue': pik_color('blue', 0),
            'orange': pik_color('orange', 0),
            'green': pik_color('green', -1),
            'gray': pik_color('gray', -1)}

pik_colors = map(lambda i: pik_color(
    ['blue', 'green', 'orange', 'gray'][int(i % 4)], [0, 3, -3, 1, -1, 2, -2][int(i / 4)]), range(20))


def pik_color_list(n_of_elements, col_list=[pik_color('green'), pik_color('blue'), pik_color('orange')]):
    c_map = LinearSegmentedColormap.from_list('my_colors', col_list, N=n_of_elements)
    return [(c_map(1. * i / (n_of_elements - 1))) for i in np.arange(0, n_of_elements)]


agent_colors = pik_color_list(5)
import matplotlib.colors

agent_colors_list = [matplotlib.colors.to_hex(i_color) for i_color in agent_colors]

# define maps to map numerical dimensions back to keys
region_names = ['AFG',
                'ALB',
                'DZA',
                'AND',
                'AGO',
                'ATG',
                'ARG',
                'ARM',
                'ABW',
                'AUS',
                'AUT',
                'AZE',
                'BHS',
                'BHR',
                'BGD',
                'BRB',
                'BLR',
                'BEL',
                'BLZ',
                'BEN',
                'BMU',
                'BTN',
                'BOL',
                'BIH',
                'BWA',
                'BRA',
                'VGB',
                'BRN',
                'BGR',
                'BFA',
                'BDI',
                'KHM',
                'CMR',
                'CAN',
                'CPV',
                'CYM',
                'CAF',
                'TCD',
                'CHL',
                'CN.AH',
                'CN.BJ',
                'CN.CQ',
                'CN.FJ',
                'CN.GS',
                'CN.GD',
                'CN.GX',
                'CN.GZ',
                'CN.HA',
                'CN.HB',
                'CN.HL',
                'CN.HE',
                'CN.HU',
                'CN.HN',
                'CN.JS',
                'CN.JX',
                'CN.JL',
                'CN.LN',
                'CN.NM',
                'CN.NX',
                'CN.QH',
                'CN.SA',
                'CN.SD',
                'CN.SH',
                'CN.SX',
                'CN.SC',
                'CN.TJ',
                'CN.XJ',
                'CN.XZ',
                'CN.YN',
                'CN.ZJ',
                'COL',
                'COG',
                'CRI',
                'HRV',
                'CUB',
                'CYP',
                'CZE',
                'CIV',
                'PRK',
                'COD',
                'DNK',
                'DJI',
                'DOM',
                'ECU',
                'EGY',
                'SLV',
                'ERI',
                'EST',
                'ETH',
                'FJI',
                'FIN',
                'FRA',
                'PYF',
                'GAB',
                'GMB',
                'GEO',
                'DEU',
                'GHA',
                'GRC',
                'GRL',
                'GTM',
                'GIN',
                'GUY',
                'HTI',
                'HND',
                'HKG',
                'HUN',
                'ISL',
                'IND',
                'IDN',
                'IRN',
                'IRQ',
                'IRL',
                'ISR',
                'ITA',
                'JAM',
                'JPN',
                'JOR',
                'KAZ',
                'KEN',
                'KWT',
                'KGZ',
                'LAO',
                'LVA',
                'LBN',
                'LSO',
                'LBR',
                'LBY',
                'LIE',
                'LTU',
                'LUX',
                'MAC',
                'MDG',
                'MWI',
                'MYS',
                'MDV',
                'MLI',
                'MLT',
                'MRT',
                'MUS',
                'MEX',
                'MCO',
                'MNG',
                'MNE',
                'MAR',
                'MOZ',
                'MMR',
                'NAM',
                'NPL',
                'NLD',
                'ANT',
                'NCL',
                'NZL',
                'NIC',
                'NER',
                'NGA',
                'NOR',
                'PSE',
                'OMN',
                'PAK',
                'PAN',
                'PNG',
                'PRY',
                'PER',
                'PHL',
                'POL',
                'PRT',
                'QAT',
                'KOR',
                'MDA',
                'ROU',
                'RUS',
                'RWA',
                'WSM',
                'SMR',
                'STP',
                'SAU',
                'SEN',
                'SRB',
                'SYC',
                'SLE',
                'SGP',
                'SVK',
                'SVN',
                'SOM',
                'ZAF',
                'SSD',
                'ESP',
                'LKA',
                'SDN',
                'SUR',
                'SWZ',
                'SWE',
                'CHE',
                'SYR',
                'TWN',
                'TJK',
                'THA',
                'MKD',
                'TGO',
                'TTO',
                'TUN',
                'TUR',
                'TKM',
                'UGA',
                'UKR',
                'ARE',
                'GBR',
                'TZA',
                'US.AL',
                'US.AK',
                'US.AZ',
                'US.AR',
                'US.CA',
                'US.CO',
                'US.CT',
                'US.DE',
                'US.DC',
                'US.FL',
                'US.GA',
                'US.HI',
                'US.ID',
                'US.IL',
                'US.IN',
                'US.IA',
                'US.KS',
                'US.KY',
                'US.LA',
                'US.ME',
                'US.MD',
                'US.MA',
                'US.MI',
                'US.MN',
                'US.MS',
                'US.MO',
                'US.MT',
                'US.NE',
                'US.NV',
                'US.NH',
                'US.NJ',
                'US.NM',
                'US.NY',
                'US.NC',
                'US.ND',
                'US.OH',
                'US.OK',
                'US.OR',
                'US.PA',
                'US.RI',
                'US.SC',
                'US.SD',
                'US.TN',
                'US.TX',
                'US.UT',
                'US.VT',
                'US.VA',
                'US.WA',
                'US.WV',
                'US.WI',
                'US.WY',
                'URY',
                'UZB',
                'VUT',
                'VEN',
                'VNM',
                'YEM',
                'ZMB',
                'ZWE']

region_indexes = list(range(0, len(region_names) + 1))
regions_index_name_dict = dict(zip(region_indexes, region_names))
regions_name_index_dict = dict(zip(region_names, region_indexes))


# WORLD BANK region definitions & income groups based on 2021 data, generated with script "translate_world_bank_category_data.py"

#N.B. Venezuela (VEN) not categorized
world_bank_income_groups = {'High income': ['ABW', 'AND', 'ARE', 'ATG', 'AUS', 'AUT', 'BEL', 'BHR', 'BHS', 'BMU', 'BRB', 'BRN', 'CAN', 'CHE', 'CHI', 'CHL', 'CUW', 'CYM', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'FRO', 'GBR', 'GIB', 'GRC', 'GRL', 'GUM', 'HKG', 'HRV', 'HUN', 'IMN', 'IRL', 'ISL', 'ISR', 'ITA', 'JPN', 'KNA', 'KOR', 'KWT', 'LIE', 'LTU', 'LUX', 'LVA', 'MAC', 'MAF', 'MCO', 'MLT', 'MNP', 'NCL', 'NLD', 'NOR', 'NRU', 'NZL', 'OMN', 'PAN', 'POL', 'PRI', 'PRT', 'PYF', 'QAT', 'ROU', 'SAU', 'SGP', 'SMR', 'SVK', 'SVN', 'SWE', 'SXM', 'SYC', 'TCA', 'TTO', 'TWN', 'URY', 'USA', 'VGB', 'VIR']
    , 'Low income': ['AFG', 'BDI', 'BFA', 'CAF', 'COD', 'ERI', 'ETH', 'GIN', 'GMB', 'GNB', 'LBR', 'MDG', 'MLI', 'MOZ', 'MWI', 'NER', 'PRK', 'RWA', 'SDN', 'SLE', 'SOM', 'SSD', 'SYR', 'TCD', 'TGO', 'UGA', 'YEM', 'ZMB'],
 'Lower middle income': ['AGO', 'BEN', 'BGD', 'BOL', 'BTN', 'CIV', 'CMR', 'COG', 'COM', 'CPV', 'DJI', 'DZA', 'EGY', 'FSM', 'GHA', 'HND', 'HTI', 'IDN', 'IND', 'IRN', 'KEN', 'KGZ', 'KHM', 'KIR', 'LAO', 'LBN', 'LKA', 'LSO', 'MAR', 'MMR', 'MNG', 'MRT', 'NGA', 'NIC', 'NPL', 'PAK', 'PHL', 'PNG', 'PSE', 'SEN', 'SLB', 'SLV', 'STP', 'SWZ', 'TJK', 'TLS', 'TUN', 'TZA', 'UKR', 'UZB', 'VNM', 'VUT', 'WSM', 'ZWE']
    , 'Upper middle income': ['ALB', 'ARG', 'ARM', 'ASM', 'AZE', 'BGR', 'BIH', 'BLR', 'BLZ', 'BRA', 'BWA', 'CHN', 'COL', 'CRI', 'CUB', 'DMA', 'DOM', 'ECU', 'FJI', 'GAB', 'GEO', 'GNQ', 'GRD', 'GTM', 'GUY', 'IRQ', 'JAM', 'JOR', 'KAZ', 'LBY', 'LCA', 'MDA', 'MDV', 'MEX', 'MHL', 'MKD', 'MNE', 'MUS', 'MYS', 'NAM', 'PER', 'PLW', 'PRY', 'RUS', 'SRB', 'SUR', 'THA', 'TKM', 'TON', 'TUR', 'TUV', 'VCT', 'XKX', 'ZAF']}


world_bank_income_groups_chn_usa = {'High income': ['ABW', 'AND', 'ARE', 'ATG', 'AUS', 'AUT', 'BEL', 'BHR', 'BHS', 'BMU', 'BRB', 'BRN', 'CAN', 'CHE', 'CHI', 'CHL', 'CUW', 'CYM', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'FRO', 'GBR', 'GIB', 'GRC', 'GRL', 'GUM', 'HKG', 'HRV', 'HUN', 'IMN', 'IRL', 'ISL', 'ISR', 'ITA', 'JPN', 'KNA', 'KOR', 'KWT', 'LIE', 'LTU', 'LUX', 'LVA', 'MAC', 'MAF', 'MCO', 'MLT', 'MNP', 'NCL', 'NLD', 'NOR', 'NRU', 'NZL', 'OMN', 'PAN', 'POL', 'PRI', 'PRT', 'PYF', 'QAT', 'ROU', 'SAU', 'SGP', 'SMR', 'SVK', 'SVN', 'SWE', 'SXM', 'SYC', 'TCA', 'TTO', 'TWN', 'URY', 'USA', 'VGB', 'VIR']+WORLD_REGIONS["USA"]
    , 'Low income': ['AFG', 'BDI', 'BFA', 'CAF', 'COD', 'ERI', 'ETH', 'GIN', 'GMB', 'GNB', 'LBR', 'MDG', 'MLI', 'MOZ', 'MWI', 'NER', 'PRK', 'RWA', 'SDN', 'SLE', 'SOM', 'SSD', 'SYR', 'TCD', 'TGO', 'UGA', 'YEM', 'ZMB'],
 'Lower middle income': ['AGO', 'BEN', 'BGD', 'BOL', 'BTN', 'CIV', 'CMR', 'COG', 'COM', 'CPV', 'DJI', 'DZA', 'EGY', 'FSM', 'GHA', 'HND', 'HTI', 'IDN', 'IND', 'IRN', 'KEN', 'KGZ', 'KHM', 'KIR', 'LAO', 'LBN', 'LKA', 'LSO', 'MAR', 'MMR', 'MNG', 'MRT', 'NGA', 'NIC', 'NPL', 'PAK', 'PHL', 'PNG', 'PSE', 'SEN', 'SLB', 'SLV', 'STP', 'SWZ', 'TJK', 'TLS', 'TUN', 'TZA', 'UKR', 'UZB', 'VNM', 'VUT', 'WSM', 'ZWE']
    , 'Upper middle income': ['ALB', 'ARG', 'ARM', 'ASM', 'AZE', 'BGR', 'BIH', 'BLR', 'BLZ', 'BRA', 'BWA', 'CHN', 'COL', 'CRI', 'CUB', 'DMA', 'DOM', 'ECU', 'FJI', 'GAB', 'GEO', 'GNQ', 'GRD', 'GTM', 'GUY', 'IRQ', 'JAM', 'JOR', 'KAZ', 'LBY', 'LCA', 'MDA', 'MDV', 'MEX', 'MHL', 'MKD', 'MNE', 'MUS', 'MYS', 'NAM', 'PER', 'PLW', 'PRY', 'RUS', 'SRB', 'SUR', 'THA', 'TKM', 'TON', 'TUR', 'TUV', 'VCT', 'XKX', 'ZAF']+WORLD_REGIONS["CHN"]}


world_bank_region_groups = {'Latin America & Caribbean': ['ABW', 'ARG', 'ATG', 'BHS', 'BLZ', 'BOL', 'BRA', 'BRB', 'CHL', 'COL', 'CRI', 'CUB', 'CUW', 'CYM', 'DMA', 'DOM', 'ECU', 'GRD', 'GTM', 'GUY', 'HND', 'HTI', 'JAM', 'KNA', 'LCA', 'MAF', 'MEX', 'NIC', 'PAN', 'PER', 'PRI', 'PRY', 'SLV', 'SUR', 'SXM', 'TCA', 'TTO', 'URY', 'VCT', 'VEN', 'VGB', 'VIR']
    , 'South Asia': ['AFG', 'BGD', 'BTN', 'IND', 'LKA', 'MDV', 'NPL', 'PAK']
    , 'Sub-Saharan Africa': ['AGO', 'BDI', 'BEN', 'BFA', 'BWA', 'CAF', 'CIV', 'CMR', 'COD', 'COG', 'COM', 'CPV', 'ERI', 'ETH', 'GAB', 'GHA', 'GIN', 'GMB', 'GNB', 'GNQ', 'KEN', 'LBR', 'LSO', 'MDG', 'MLI', 'MOZ', 'MRT', 'MUS', 'MWI', 'NAM', 'NER', 'NGA', 'RWA', 'SDN', 'SEN', 'SLE', 'SOM', 'SSD', 'STP', 'SWZ', 'SYC', 'TCD', 'TGO', 'TZA', 'UGA', 'ZAF', 'ZMB', 'ZWE']
    , 'Europe & Central Asia': ['ALB', 'AND', 'ARM', 'AUT', 'AZE', 'BEL', 'BGR', 'BIH', 'BLR', 'CHE', 'CHI', 'CYP', 'CZE', 'DEU', 'DNK', 'ESP', 'EST', 'FIN', 'FRA', 'FRO', 'GBR', 'GEO', 'GIB', 'GRC', 'GRL', 'HRV', 'HUN', 'IMN', 'IRL', 'ISL', 'ITA', 'KAZ', 'KGZ', 'LIE', 'LTU', 'LUX', 'LVA', 'MCO', 'MDA', 'MKD', 'MNE', 'NLD', 'NOR', 'POL', 'PRT', 'ROU', 'RUS', 'SMR', 'SRB', 'SVK', 'SVN', 'SWE', 'TJK', 'TKM', 'TUR', 'UKR', 'UZB', 'XKX']
    , 'Middle East & North Africa': ['ARE', 'BHR', 'DJI', 'DZA', 'EGY', 'IRN', 'IRQ', 'ISR', 'JOR', 'KWT', 'LBN', 'LBY', 'MAR', 'MLT', 'OMN', 'PSE', 'QAT', 'SAU', 'SYR', 'TUN', 'YEM']
    , 'East Asia & Pacific': ['ASM', 'AUS', 'BRN', 'CHN', 'FJI', 'FSM', 'GUM', 'HKG', 'IDN', 'JPN', 'KHM', 'KIR', 'KOR', 'LAO', 'MAC', 'MHL', 'MMR', 'MNG', 'MNP', 'MYS', 'NCL', 'NRU', 'NZL', 'PHL', 'PLW', 'PNG', 'PRK', 'PYF', 'SGP', 'SLB', 'THA', 'TLS', 'TON', 'TUV', 'TWN', 'VNM', 'VUT', 'WSM']
    , 'North America': ['BMU', 'CAN', 'USA']}





def region_map(region_index):
    return regions_index_name_dict[region_index]


consumer_names = ["1st", "2nd", "3rd", "4th", "5th"]
consumer_indizes = range(0, 5)

sector_names = ['AGRI',
                'FISH',
                'MINQ',
                'FOOD',
                'TEXL',
                'WOOD',
                'OILC',
                'METL',
                'MACH',
                'TREQ',
                'MANU',
                'RECY',
                'ELWA',
                'CONS',
                'REPA',
                'WHOT',
                'RETT',
                'GAST',
                'TRAN',
                'COMM',
                'FINC',
                'ADMI',
                'EDHE',
                'HOUS',
                'OTHE',
                'REXI']
sector_indizes = range(0, 26)

#DOSE sectors groups

dose_sector_groups = {"Agriculture":["AGRI","FISH"]
    ,"Manufacture":["FOOD","ELWA","OILC","WOOD","TEXT","METL","MACH","TREQ","MANU","CONS","MINQ","RECY"]
    ,"Services":["EDHE","TRAN","COMM","RETT","WHOT","GAST","REXI","OTHERS","FINC","HOUS","ADMI","REPA"]}


consumer_index_name_dict = dict(zip(consumer_indizes, consumer_names))
consumer_name_index_dict = dict(zip(consumer_names, consumer_indizes))


def consumer_map(consumer_index):
    return consumer_index_name_dict[consumer_index]


producing_sectors_index_name_dict = dict(zip(sector_indizes, sector_names))
producing_sectors_name_index_dict = dict(zip(sector_names, sector_indizes))


def producing_sector_map(sector_index):
    return producing_sectors_index_name_dict[sector_index]


# helper functions for basket considerations

basket_names = list(consumption_baskets.keys())
number_baskets = len(basket_names)
indizes_baskets = range(0, number_baskets)
consumption_baskets_name_index = dict(zip(basket_names, indizes_baskets))
consumption_baskets_index_name = dict(zip(indizes_baskets, basket_names))


def basket_map(basket_index):
    return consumption_baskets_index_name[basket_index]


import math
golden = (1 + math.sqrt(5)) / 2

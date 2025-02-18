from iso3166 import countries_by_alpha3
from pycountry_convert import country_alpha3_to_continent_code, convert_continent_code_to_continent_name

# TODO important definitions such as standard region sets
# TODO: unify, maybe base on *.yml lists of individual regions for easier sharing and use outside of this codebase


world_regions_keys = [
    "AFR", "ASI", "EUR", "LAM", "NAM", "OCE", "ADB", "EU28", "EU27", "OECD", "BRICS", "CHN", "USA", "G20", "G20_REST", "BRIS"
]

continent_region_groups = ["AFR", "ASI", "EUR", "LAM", "NAM", "OCE"]

#generate a dict of continental definitions using iso3166 codes
continent_definitions = {}
for region in continent_region_groups:
    continent_definitions[region] = []

for country_code in countries_by_alpha3.keys():
    try:
        continent_code = country_alpha3_to_continent_code(country_code)
        continent_name = convert_continent_code_to_continent_name(continent_code)
        for region in continent_region_groups:
            if region[:3].upper() in continent_name.upper():
                continent_definitions[region].append(country_code)
    except KeyError:
        continue
    
#TODO: generate dictonaries of political groups like EU, OECD, G20, BRICS
    
# WORLD BANK region definitions & income groups based on 2021 data

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
  
eora_region_names = [
    'AFG', 'ALB', 'DZA', 'AND', 'AGO', 'ATG', 'ARG', 'ARM', 'ABW', 'AUS', 'AUT', 'AZE', 'BHS', 'BHR', 'BGD', 'BRB', 'BLR', 
    'BEL', 'BLZ', 'BEN', 'BMU', 'BTN', 'BOL', 'BIH', 'BWA', 'BRA', 'VGB', 'BRN', 'BGR', 'BFA', 'BDI', 'KHM', 'CMR', 'CAN', 
    'CPV', 'CYM', 'CAF', 'TCD', 'CHL', 'CN.AH', 'CN.BJ', 'CN.CQ', 'CN.FJ', 'CN.GS', 'CN.GD', 'CN.GX', 'CN.GZ', 'CN.HA', 
    'CN.HB', 'CN.HL', 'CN.HE', 'CN.HU', 'CN.HN', 'CN.JS', 'CN.JX', 'CN.JL', 'CN.LN', 'CN.NM', 'CN.NX', 'CN.QH', 'CN.SA', 
    'CN.SD', 'CN.SH', 'CN.SX', 'CN.SC', 'CN.TJ', 'CN.XJ', 'CN.XZ', 'CN.YN', 'CN.ZJ', 'COL', 'COG', 'CRI', 'HRV', 'CUB', 
    'CYP', 'CZE', 'CIV', 'PRK', 'COD', 'DNK', 'DJI', 'DOM', 'ECU', 'EGY', 'SLV', 'ERI', 'EST', 'ETH', 'FJI', 'FIN', 'FRA', 
    'PYF', 'GAB', 'GMB', 'GEO', 'DEU', 'GHA', 'GRC', 'GRL', 'GTM', 'GIN', 'GUY', 'HTI', 'HND', 'HKG', 'HUN', 'ISL', 'IND', 
    'IDN', 'IRN', 'IRQ', 'IRL', 'ISR', 'ITA', 'JAM', 'JPN', 'JOR', 'KAZ', 'KEN', 'KWT', 'KGZ', 'LAO', 'LVA', 'LBN', 'LSO', 
    'LBR', 'LBY', 'LIE', 'LTU', 'LUX', 'MAC', 'MDG', 'MWI', 'MYS', 'MDV', 'MLI', 'MLT', 'MRT', 'MUS', 'MEX', 'MCO', 'MNG', 
    'MNE', 'MAR', 'MOZ', 'MMR', 'NAM', 'NPL', 'NLD', 'ANT', 'NCL', 'NZL', 'NIC', 'NER', 'NGA', 'NOR', 'PSE', 'OMN', 'PAK', 
    'PAN', 'PNG', 'PRY', 'PER', 'PHL', 'POL', 'PRT', 'QAT', 'KOR', 'MDA', 'ROU', 'RUS', 'RWA', 'WSM', 'SMR', 'STP', 'SAU', 
    'SEN', 'SRB', 'SYC', 'SLE', 'SGP', 'SVK', 'SVN', 'SOM', 'ZAF', 'SSD', 'ESP', 'LKA', 'SDN', 'SUR', 'SWZ', 'SWE', 'CHE', 
    'SYR', 'TWN', 'TJK', 'THA', 'MKD', 'TGO', 'TTO', 'TUN', 'TUR', 'TKM', 'UGA', 'UKR', 'ARE', 'GBR', 'TZA', 'US.AL', 
    'US.AK', 'US.AZ', 'US.AR', 'US.CA', 'US.CO', 'US.CT', 'US.DE', 'US.DC', 'US.FL', 'US.GA', 'US.HI', 'US.ID', 'US.IL', 
    'US.IN', 'US.IA', 'US.KS', 'US.KY', 'US.LA', 'US.ME', 'US.MD', 'US.MA', 'US.MI', 'US.MN', 'US.MS', 'US.MO', 'US.MT', 
    'US.NE', 'US.NV', 'US.NH', 'US.NJ', 'US.NM', 'US.NY', 'US.NC', 'US.ND', 'US.OH', 'US.OK', 'US.OR', 'US.PA', 'US.RI', 
    'US.SC', 'US.SD', 'US.TN', 'US.TX', 'US.UT', 'US.VT', 'US.VA', 'US.WA', 'US.WV', 'US.WI', 'US.WY', 'URY', 'UZB', 
    'VUT', 'VEN', 'VNM', 'YEM', 'ZMB', 'ZWE'
]

eora_region_indexes = list(range(0, len(eora_region_names) + 1))
eora_region_index_to_name_ = dict(zip(eora_region_indexes, eora_region_names))
eora_region_name_to_index_ = dict(zip(eora_region_names, eora_region_indexes))  
  

# consumption baskets for consumer analysis based on EORA
necessary = ["AGRI", "FOOD", "ELWA", "FISH", "EDHE"]
relevant = ["OILC", "TRAN", "WOOD", "COMM", "RETT", "TEXL", "METL", "MACH", "TREQ", "MANU", "CONS", "WHOT"]
other = ["GAST", "MINQ", "REXI", "RECY", "OTHE", "FINC", "HOUS", "ADMI", "REPA"]
consumption_baskets = {"necessary": necessary, "relevant": relevant, "other": other}

basket_names = list(consumption_baskets.keys())
number_baskets = len(basket_names)
indizes_baskets = range(0, number_baskets)
consumption_baskets_name_to_index = dict(zip(basket_names, indizes_baskets))
consumption_baskets_index_to_name = dict(zip(indizes_baskets, basket_names))

#income quintile names
long_quintiles = ["first_income_quintile", "second_income_quintile", "third_income_quintile", "fourth_income_quintile", "fifth_income_quintile"]
short_quintiles = ["q1", "q2", "q3", "q4", "q5"]
consumer_indizes = range(0, 5)

consumer_long_to_short = dict(zip(long_quintiles, short_quintiles))
consumer_short_to_long = dict(zip(short_quintiles, long_quintiles))
consumer_index_to_short = dict(zip(consumer_indizes, short_quintiles))
consumer_short_to_index = dict(zip(short_quintiles, consumer_indizes))

sector_names = ['AGRI', 'FISH', 'MINQ', 'FOOD', 'TEXL', 'WOOD', 'OILC', 'METL','MACH',
                'TREQ','MANU','RECY','ELWA','CONS','REPA','WHOT','RETT','GAST','TRAN',
                'COMM','FINC','ADMI','EDHE','HOUS','OTHE','REXI']

sector_indizes = range(0, 26)
producing_sectors_index_to_name_ = dict(zip(sector_indizes, sector_names))
producing_sectors_name_to_index = dict(zip(sector_names, sector_indizes))


#DOSE sectors groups
dose_sector_groups = {"Agriculture":["AGRI","FISH"]
    ,"Manufacture":["FOOD","ELWA","OILC","WOOD","TEXT","METL","MACH","TREQ","MANU","CONS","MINQ","RECY"]
    ,"Services":["EDHE","TRAN","COMM","RETT","WHOT","GAST","REXI","OTHERS","FINC","HOUS","ADMI","REPA"]}


# 

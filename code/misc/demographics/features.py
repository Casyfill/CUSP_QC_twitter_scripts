#!/usr/bin/env python
#-*- coding: utf-8 -*-

featuresets = {
    'population': {'SE_T009_002': 'Under 18',
                   'SE_T009_003': 'Youth 18-34',
                   'SE_T009_004': 'Adult 35-64',
                   'SE_T009_005': 'Senior 65+'
                   # 'SE_T001_001' is the Total population
                   },
    'race': 	{
        'SE_T013_002': 'White',
        'SE_T013_003': 'Black or African',
        'SE_T013_004': 'American Indian and Alaska Native',
        'SE_T013_005': 'Asian',
        'SE_T013_006': 'Native Hawaiian and Other Pacific Islander',
        'SE_T013_007': 'Some Other',
        'SE_T013_008': 'Two or More races'
    },

    'households': {
        'SE_T017_002': 'Family',
        'SE_T017_003': 'Married-couple',
        'SE_T017_004': 'Single-parent',
        'SE_T017_007': 'No Family',
        'SE_T018_002': 'Household With People Under 18'
    },
    'education': {
		'SE_T025_002': '< High School',
		'SE_T025_003': 'High School',
		'SE_T025_004': 'Some College',
		'SE_T025_005': 'Bachelor',
		'SE_T025_006': 'Master',
		'SE_T025_007': 'Professional School',
		'SE_T025_008': 'Doctorate'
    },
    'insurance': dict((x, y) for x, y in zip(['SE_T145_002', 'SE_T145_003', 'SE_T145_004', 'SE_T145_005'], ['No Coverage', 'Some Coverage', 'Public Insurance', 'Private Insurance'])),

    'born': dict((x, y) for x, y in zip(['SE_T133_002', 'SE_T133_004', 'SE_T133_005'], ['Native Born', 'Foreign Born: Naturalized Citizen',
               'Foreign Born: Not a Citizen'])),
    'commute': dict((x, y) for x, y in zip(['SE_T129_003', 'SE_T129_004', 'SE_T129_005',
                'SE_T129_006', 'SE_T129_007', 'SE_T129_008', 'SE_T129_009', 'SE_T129_010'], ['< 10 Minutes', 'Communte 10~19 Minutes', 'Communte 20~29 Minutes', 'Communte 30~39 Minutes',
                       'Communte 40~59 Minutes', 'Communte 60~89 Minutes', '> 90 Minutes', 'Work At Home'])),
    'income': dict((x, y) for x, y in zip(['SE_T056_002', 'SE_T056_003', 'SE_T056_004', 'SE_T056_005', 'SE_T056_006', 'SE_T056_007',
          'SE_T056_008', 'SE_T056_009', 'SE_T056_010', 'SE_T056_011', 'SE_T056_012', 'SE_T056_013',
          'SE_T056_014', 'SE_T056_015', 'SE_T056_016', 'SE_T056_017'], ['Income < \$10,000', 'Income \$10,000 to \$14,999', 'Income \$15,000 to \$19,999', 'Income \$20,000 to \$24,999',
                 'Income \$25,000 to \$29,999', 'Income \$30,000 to \$34,999', 'Income \$35,000 to \$39,999', 'Income \$40,000 to \$44,999',
                 'Income \$45,000 to \$49,999', 'Income \$50,000 to \$59,999', 'Income \$60,000 to \$74,999', 'Income \$75,000 to \$99,999',
                 'Income \$100,000 to \$124,999', 'Income \$125,000 to \$149,999', 'Income \$150,000 to \$199,999', 'Income >= \$200,000'])),
    'hh_price': dict((x, y) for x, y in zip(['SE_T100_002', 'SE_T100_003', 'SE_T100_004', 'SE_T100_005',
               'SE_T100_006', 'SE_T100_007', 'SE_T100_008', 'SE_T100_009', 'SE_T100_010'], ['House Price < $20,000', 'House Price \$20,000 to \$49,999', 'House Price \$50,000 to \$99,999', 'House Price \$100,000 to \$149,999',
                      'House Price \$150,000 to \$299,999', 'House Price \$300,000 to \$499,999', 'House Price \$500,000 to \$749,999', 'House Price \$750,000 to \$999,999',
                      'House Price >= \$1,000,000'])),
    'rent_price': dict((x, y) for x, y in zip(['SE_T102_002', 'SE_T102_003', 'SE_T102_004', 'SE_T102_005', 'SE_T102_006', 'SE_T102_007', 'SE_T102_008', 'SE_T102_009'], ['Rent < $300', 'Rent \$300 to \$599', 'Rent \$600 to \$799', 'Rent \$800 to \$999', 'Rent \$1,000 to \$1,249',
                     'Rent \$1,250 to \$1,499', 'Rent \$1,500 to \$1,999', 'Rent >= \$2,000']
	)),
    'employment': dict((x, y) for x, y in zip(['SE_T037_002', 'SE_T037_003'], ['Employed', 'Unemployed'])),

    'houseType': dict((x, y) for x, y in zip(['SE_T095_003', 'SE_T094_002', 'SE_T094_003'], ['Vacant Houses', 'Owner Occupied', 'Renter Occupied']))

}

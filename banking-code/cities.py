import numpy as np

def get_cities_data():
    # Pre-compute provinces and cities
    provinces = [
        'Gauteng', 'KwaZulu-Natal', 'Western Cape', 'Eastern Cape', 'Limpopo',
        'Mpumalanga', 'Free State', 'Northern Cape', 'North West'
    ]

    cities = {
        'Gauteng': [
            'Johannesburg', 'Pretoria', 'Soweto', 'Randburg', 'Roodepoort', 'Benoni',
            'Germiston', 'Boksburg', 'Kempton Park', 'Sandton', 'Midrand', 'Centurion',
            'Vanderbijlpark', 'Vereeniging', 'Springs', 'Alberton', 'Edenvale', 'Bedfordview'
        ],
        'KwaZulu-Natal': [
            'Durban', 'Pietermaritzburg', 'Umhlanga', 'Pinetown', 'Richards Bay',
            'Newcastle', 'Ladysmith', 'Empangeni', 'Dundee', 'Vryheid', 'Estcourt',
            'Kokstad', 'Port Shepstone', 'Stanger', 'Ixopo', 'Howick'
        ],
        'Western Cape': [
            'Cape Town', 'Stellenbosch', 'Paarl', 'Worcester', 'George', 'Knysna',
            'Mossel Bay', 'Oudtshoorn', 'Wellington', 'Malmesbury', 'Hermanus',
            'Swellendam', 'Caledon', 'Robertson', 'Bredasdorp', 'Vredenburg'
        ],
        'Eastern Cape': [
            'Port Elizabeth', 'East London', 'Grahamstown', 'Queenstown', 'Mthatha',
            'King Williams Town', 'Uitenhage', 'Alice', 'Fort Beaufort', 'Cradock',
            'Graaff-Reinet', 'Port Alfred', 'Somerset East', 'Stutterheim', 'Bisho'
        ],
        'Limpopo': [
            'Polokwane', 'Thohoyandou', 'Tzaneen', 'Lephalale', 'Mokopane', 'Giyani',
            'Musina', 'Louis Trichardt', 'Bela-Bela', 'Hoedspruit', 'Phalaborwa',
            'Thabazimbi', 'Modimolle', 'Marble Hall', 'Dendron'
        ],
        'Mpumalanga': [
            'Nelspruit', 'Witbank', 'Middleburg', 'Secunda', 'Barberton', 'Standerton',
            'Ermelo', 'Volksrust', 'Lydenburg', 'White River', 'Sabie', 'Graskop',
            'Hazyview', 'Komatipoort', 'Carolina', 'Bethal'
        ],
        'Free State': [
            'Bloemfontein', 'Welkom', 'Kroonstad', 'Bethlehem', 'Sasolburg', 'Odendaalsrus',
            'Parys', 'Vredefort', 'Heilbron', 'Harrismith', 'Ficksburg', 'Phuthaditjhaba',
            'Virginia', 'Hennenman', 'Senekal', 'Reitz'
        ],
        'Northern Cape': [
            'Kimberley', 'Upington', 'Springbok', 'De Aar', 'Kuruman', 'Postmasburg',
            'Calvinia', 'Prieska', 'Carnarvon', 'Britstown', 'Colesberg', 'Hanover',
            'Victoria West', 'Hopetown', 'Douglas', 'Oranjemund'
        ],
        'North West': [
            'Mahikeng', 'Rustenburg', 'Potchefstroom', 'Klerksdorp', 'Brits', 'Zeerust',
            'Lichtenburg', 'Vryburg', 'Schweizer-Reneke', 'Christiana', 'Coligny',
            'Koster', 'Madikwe', 'Taung', 'Stella', 'Ganyesa'
        ]
    }

    # Province weights based on South African population distribution
    province_probs = np.array([0.24, 0.19, 0.12, 0.12, 0.10, 0.08, 0.05, 0.02, 0.08])

    return provinces, cities, province_probs
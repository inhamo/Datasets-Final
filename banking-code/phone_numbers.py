from faker import Faker
import random

PHONE_PLANS = {
    'South Africa': {'cc': '+27', 'nsn_length': 9, 'mobile_prefixes': ['60','61','62','63','64','65','66','67','68','71','72','73','74','76','78','79','81','82','83','84'], 'faker_locale': 'zu_ZA'},
    'United Kingdom': {'cc': '+44', 'nsn_length': 10, 'mobile_prefixes': ['7'], 'faker_locale': 'en_GB'},
    'United States': {'cc': '+1', 'nsn_length': 10, 'mobile_prefixes': ['2','3','4','5','6','7','8','9'], 'faker_locale': 'en_US'},
    'Canada': {'cc': '+1', 'nsn_length': 10, 'mobile_prefixes': ['2','3','4','5','6','7','8','9'], 'faker_locale': 'en_CA'},
    'Germany': {'cc': '+49', 'nsn_length': 10, 'mobile_prefixes': ['15','16','17'], 'faker_locale': 'de_DE'},
    'France': {'cc': '+33', 'nsn_length': 9, 'mobile_prefixes': ['6','7'], 'faker_locale': 'fr_FR'},
    'India': {'cc': '+91', 'nsn_length': 10, 'mobile_prefixes': ['6','7','8','9'], 'faker_locale': 'hi_IN'},
    'Nigeria': {'cc': '+234', 'nsn_length': 10, 'mobile_prefixes': ['70','80','81','90','91'], 'faker_locale': 'en_GB'},
    'Zimbabwe': {'cc': '+263', 'nsn_length': 9, 'mobile_prefixes': ['71','73','77','78'], 'faker_locale': 'en_GB'},
    'Kenya': {'cc': '+254', 'nsn_length': 9, 'mobile_prefixes': ['7','1'], 'faker_locale': 'en_GB'},
    'Australia': {'cc': '+61', 'nsn_length': 9, 'mobile_prefixes': ['4'], 'faker_locale': 'en_AU'},
    'Brazil': {'cc': '+55', 'nsn_length': 11, 'mobile_prefixes': ['9'], 'faker_locale': 'pt_BR'},
    'United Arab Emirates': {'cc': '+971', 'nsn_length': 9, 'mobile_prefixes': ['50','52','54','55','56','58'], 'faker_locale': 'ar_AE'},
    'Netherlands': {'cc': '+31', 'nsn_length': 9, 'mobile_prefixes': ['6'], 'faker_locale': 'nl_NL'},
    'Spain': {'cc': '+34', 'nsn_length': 9, 'mobile_prefixes': ['6','7'], 'faker_locale': 'es_ES'},
    'Italy': {'cc': '+39', 'nsn_length': 10, 'mobile_prefixes': ['3'], 'faker_locale': 'it_IT'},
    'China': {'cc': '+86', 'nsn_length': 11, 'mobile_prefixes': ['13','14','15','16','17','18','19'], 'faker_locale': 'zh_CN'},
    'Japan': {'cc': '+81', 'nsn_length': 10, 'mobile_prefixes': ['70','80','90'], 'faker_locale': 'ja_JP'},
}

def generate_phone_number(nationality, faker_instances):
    plan = PHONE_PLANS.get(nationality, PHONE_PLANS['South Africa'])
    locale = plan['faker_locale']
    fake = faker_instances.get(locale, Faker(locale))
    faker_instances[locale] = fake

    prefix = random.choice(plan['mobile_prefixes'])
    nsn_length = plan['nsn_length'] - len(prefix)
    nsn = prefix + ''.join([str(random.randint(0, 9)) for _ in range(nsn_length)])
    return f"{plan['cc']}{nsn}"
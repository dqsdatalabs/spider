import scrapy
import requests
import json
from python_spiders.loaders import ListingLoader
from python_spiders.helper import remove_white_spaces, sq_feet_to_meters

class CitysearchcalgarySpider(scrapy.Spider):
    name = 'citysearchcalgary'
    execution_type = 'testing'
    country = 'canada'
    locale = 'en'
    external_source = f"{name.capitalize()}_PySpider_{country}_{locale}"
    def start_requests(self):
        key_headers = {'Accept': 'application/json, text/plain, */*',
             'Accept-Encoding': 'gzip, deflate, br',
             'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
             'Authorization': 'Apikey 0KNMIYtQSeKAZvGRaBAQXzygpnlhZwY',
             'Connection': 'keep-alive',
             'Host': 'connect.propertyware.com',
             'Origin': 'https://www.citysearchcalgary.com',
             'Referer': 'https://www.citysearchcalgary.com/',
             'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
             'sec-ch-ua-mobile': '?0',
             'sec-ch-ua-platform': 'Linux',
             'Sec-Fetch-Dest': 'empty',
             'Sec-Fetch-Mode': 'cors',
             'Sec-Fetch-Site': 'cross-site',
             'User-Agent': 'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'}
        url = "https://connect.propertyware.com/auth/apikey"
        res = requests.post(url,headers=key_headers)

        
        headers = {
            'Accept':'application/json, text/plain, */*',
            'Accept-Encoding': 'gzip, deflate, br',
            'Accept-Language': 'en-US,en;q=0.9,ar;q=0.8',
            'Authorization':res.text.split(":")[1].replace('"',"").replace("}",""),
            'Connection': 'keep-alive',
            'Host':'connect.propertyware.com',
            'Origin': 'https://www.citysearchcalgary.com',
            'Referer':'https://www.citysearchcalgary.com/',
            'sec-ch-ua': '"Chromium";v="94", "Google Chrome";v="94", ";Not A Brand";v="99"',
            'sec-ch-ua-mobile': '?0',
            'sec-ch-ua-platform':'Linux',
            'Sec-Fetch-Dest':'empty',
            'Sec-Fetch-Mode':'cors',
            'Sec-Fetch-Site':'cross-site',
            'User-Agent':'Mozilla/5.0 (X11; Linux x86_64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/94.0.4606.81 Safari/537.36'
        }
        url = 'https://connect.propertyware.com/api/marketing/listings?website_id=179273732&widget_id=2087&include_for_rent=true&include_for_sale=false'
        
        
        
        yield scrapy.Request(url=url, headers=headers, callback=self.parse)
    
    def parse(self, response):
        data = json.loads(response.text)
        aparts = ['condo','fourplex']
        houses = ['house']
        for ad in data:
            parking, balacony, washing_machine, furnished, dishwasher= fetch_amenities(ad['amenities'])
            
            property_type = ad['property_type']
            for apart in aparts:
                if apart in ad['property_type'].lower():
                    property_type = 'apartment'
            
            for house in houses:
                if house in ad['property_type'].lower():
                    property_type = 'apartment'
            
            if ad['target_deposit']:
                deposit = ad['target_deposit'].replace("$","").replace(",",'').split(".")[0]

            images = [i['original_image_url'] for i in ad['images']]
            ex_id = str(ad['id'])
            
            item = ListingLoader(response=response)
            if ad['total_area']:
                item.add_value("square_meters",         int(int(sq_feet_to_meters(ad['total_area']))*10.764))
            
            item.add_value("external_source"        , self.external_source)
            item.add_value("external_link",         'https://www.citysearchcalgary.com/calgary-homes-for-rent/property/'+ex_id)
            item.add_value("external_id",           ex_id)
            item.add_value("address",               ad['address'])
            item.add_value("available_date",        ad['available_date'])
            item.add_value("city",                  ad['city'])
            item.add_value("description",           remove_white_spaces(ad['description']))
            item.add_value("pets_allowed",          ad['pets_allowed'])
            item.add_value("images",                images)
            item.add_value("parking",               parking)
            item.add_value("balcony",               balacony)
            item.add_value("washing_machine",       washing_machine)
            item.add_value("furnished",             furnished)
            item.add_value("dishwasher",            dishwasher)
            item.add_value("parking",               parking)
            item.add_value("pets_allowed",          ad['pets_allowed'])
            item.add_value("latitude",              str(ad['lattitude']))
            item.add_value("longitude",             str(ad['longitude']))
            item.add_value("title",                 ad['posting_title'])
            item.add_value("room_count",            int(ad['no_bedrooms']))
            item.add_value("bathroom_count",        int(ad['no_bathrooms']))
            item.add_value("floor",                 str(ad['no_floors']))
            item.add_value("property_type",         property_type)
            item.add_value("rent",                  int(ad['target_rent']))
            item.add_value("deposit",               int(deposit))
            item.add_value("zipcode",               ad['zip'])
            item.add_value("landlord_phone",        '403.777.1770')
            item.add_value("currency",              'CAD')
            item.add_value("landlord_email",        'info@citysearchcalgary.com')
            
            
            yield item.load_item()


def fetch_amenities(vals):
    parking, balacony, washing_machine, furnished, dishwasher = '', '', '', '', ''
    for val in vals:
        if 'parking' in val['name'].lower():
            parking = True
        
        if 'balcon' in val['name'].lower():
            balacony = True
        
        if 'dishwasher' in val['name'].lower():
            dishwasher = True
        
        if 'furnished' in val['name'].lower():
            furnished = True
        
        if 'washer' in val['name'].lower():
            washing_machine = True
        
        if 'parking' in val['name'].lower() or 'garage' in val['name'].lower():
            parking = True
        
    return parking, balacony, washing_machine, furnished, dishwasher

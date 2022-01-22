# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re, json
from ..loaders import ListingLoader
from bs4 import BeautifulSoup
from python_spiders.helper import format_date
import dateparser

def getSqureMtr(text):
    list_text = re.findall(r'\d+',text)

    if len(list_text) == 3:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 2:
        output = float(list_text[0]+"."+list_text[1])
    elif len(list_text) == 1:
        output = int(list_text[0])
    else:
        output=0

    return int(output)

def cleanText(text):
    text = ''.join(text.split())
    text = re.sub(r'[^a-zA-Z0-9]', ' ', text).strip()
    return text.replace(" ","_").lower()

def cleanKey(data):
    if isinstance(data,dict):
        dic = {}
        for k,v in data.items():
            dic[cleanText(k)]=cleanKey(v)
        return dic
    else:
        return data

class SkotSpider(scrapy.Spider):
    name = "skot"
    allowed_domains = ["skot.be"]
    start_urls = (
        'http://www.skot.be/',
    )
    custom_settings = {
    "PROXY_ON": True,
    #"PROXY_PR_ON": True,
    "PASSWORD": "wmkpu9fkfzyo",
}
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        start_urls = [
            {'url': 'https://skot.be/fr/json?property_types%5B%5D=%22APARTMENT%22&rent_max=1800&rent_min=0&r=50.7808%2C3.8614%2C50.8868%2C4.8557', 'property_type': 'apartment'},
            {'url': 'https://skot.be/fr/json?property_types%5B%5D=%22APARTMENT%22&rent_max=1800&rent_min=0&r=50.5414%2C5.0935%2C50.6712%2C6.0212', 'property_type': 'apartment'},
            {'url': 'https://skot.be/fr/json?property_types%5B%5D=%22HOUSE%22&rent_max=1800&rent_min=0&r=50.7808%2C3.8614%2C50.8868%2C4.8557', 'property_type': 'house'},
            {'url': 'https://skot.be/fr/json?property_types%5B%5D=%22HOUSE%22&rent_max=1800&rent_min=0&r=50.5414%2C5.0935%2C50.6712%2C6.0212', 'property_type': 'house'}
        ]

        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        datas = json.loads(response.text)
        for data in datas:
            address = data['da']
            rent = data['r']
            latitude = str(data['ll'][0])
            longitude = str(data['ll'][1])
            link = response.urljoin(data['url'])
            yield scrapy.Request(
                url=link,
                callback=self.get_property_details,
                meta={
                    'property_type': response.meta.get('property_type'),
                    'rent': rent,
                    'latitude': latitude,
                    'longitude': longitude,
                    'address': address
                },
                dont_filter=True
            )
            
    def get_property_details(self,response):
        soup = BeautifulSoup(response.body)
        address = response.meta.get('address')
        city_zipcode = address.split(', ')[-1]
        city = city_zipcode.split(' ')[-1]
        zipcode = city_zipcode.split(' ')[0]  
        external_link = response.url
        latitude = response.meta.get('latitude')
        longitude = response.meta.get('longitude') 
        latitude = response.meta.get('latitude')
        property_type = response.meta.get('property_type')
        rent = response.meta.get('rent') 
        rent = re.sub(r'[\s]+', '', rent)
        description = response.xpath('//div[@class="Av-description"]/p//text()').extract()
        for des in description:
            if 'Bedrooms' in des:
                room_count = des.split(' : ')[-1]
            else:
                room_count = '1'
        images_regex = re.search(r'var ip = new av\.ImagePager\(\[(.*?)\]', response.text).group(1)
        try:
            images = re.findall(r'\"url\": \"(.+?)\"', images_regex)
        except:
            images = ''
        if images:
            item_loader = ListingLoader(response=response)
            if property_type:
                item_loader.add_value('property_type', property_type)
            if soup.find("table",class_="table ListingDetails"):
                temp_dic = {}
                all_tr=soup.find("table",class_="table ListingDetails").find_all("tr")

                for ech_tr in all_tr:
                    if ech_tr.find("th") and ech_tr.find("td"):
                        key = ech_tr.find("th").text.strip().replace('\xa0', '')
                        vals=ech_tr.find("td").text.strip().replace('\xa0', '')
                        temp_dic.update({key:vals})
            temp_dic = cleanKey(temp_dic)
            square_meters = ''
            if 'surface' in temp_dic:
                square_meters = temp_dic['surface']
            if 'caution' in temp_dic:
                utilities = getSqureMtr(temp_dic['caution'])
            if 'disponibilit' in temp_dic: 
                date2 = ''
                if 'Disponible' not in temp_dic['disponibilit']:
                    try:
                        date_parsed = dateparser.parse( temp_dic['disponibilit'], date_formats=["%d %B %Y"] ) 
                        date2 = date_parsed.strftime("%Y-%m-%d")
                    except:
                        try:
                            date2 =  format_date(temp_dic['disponibilit'],  '%d %B %Y')
                        except:
                            date2 = ''
            if 'meubl' in temp_dic:
                if 'non' not in temp_dic['meubl']:
                    item_loader.add_value('furnished', True)
            item_loader.add_value('external_link', external_link)
            item_loader.add_xpath('title', '//div[@class="AvContent-primary"]/h1/text()')
            item_loader.add_value('address', address)
            # item_loader.add_value('city', city)
            item_loader.add_value('zipcode', zipcode)
            if date2:
                item_loader.add_value('available_date', date2) 
            item_loader.add_xpath('description', '//div[@class="Av-description"]/p//text()')
            item_loader.add_value('rent_string', rent)
            item_loader.add_value('images', images)
            item_loader.add_value('square_meters', square_meters)
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)
            # item_loader.add_value('utilities', utilities)

            utlts = response.xpath("//th[contains(.,'Charges')]/following-sibling::td/text()").get()
            if utlts: item_loader.add_value('utilities', "".join(filter(str.isnumeric, utlts)))

            city = response.xpath("//h1/span/text()").get()
            if city: item_loader.add_value('city', city.strip().split(" ")[-1].strip())

            if room_count:
                item_loader.add_value('room_count', str(room_count))
            item_loader.add_value('landlord_name', 'Skot')
            item_loader.add_value('landlord_email', 'olly@skot.be')
            item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
            yield item_loader.load_item()    

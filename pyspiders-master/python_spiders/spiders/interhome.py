# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import scrapy
import json
import re
from ..loaders import ListingLoader

class InterhomeSpider(scrapy.Spider):
    name = "interhome"
    allowed_domains = ["www.interhome.com"]
    start_urls = (
        'http://www.interhome.com/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.interhome.nl/belgie/appartement/?page=1', 'property_type': 'apartment'},
            {'url': 'https://www.interhome.nl/belgie/vrijstaand-huis/?page=1', 'property_type': 'house'},
            {'url': 'https://www.interhome.nl/belgie/villa/?page=1', 'property_type': 'house'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, 
                meta={'property_type': url.get('property_type')},
                dont_filter=True
            )
    
    
    def parse(self, response, **kwargs):
        for page in range(1, 11):
            link = response.url.replace('page=1', 'page={}'.format(page))
            yield scrapy.Request(url=link, callback=self.get_detail_urls, dont_filter=True, meta={'property_type': response.meta.get('property_type')})   
        
    def get_detail_urls(self, response):
        links = response.xpath('//article[contains(@id, "objectIdx")]')
        for link in links: 
            url = response.urljoin(link.xpath('.//h4[@class="c-object__title"]/a/@href').extract_first())
            price = link.xpath('.//strong[@class="c-object__price__content"]/text()').extract_first()
            yield scrapy.Request(url=url, callback=self.get_property_details, dont_filter=True, meta={'property_type': response.meta.get('property_type'), 'price': price})
    
    def get_property_details(self, response):
        external_link = response.url
        property_type = response.meta.get('property_type')
        rent_string = response.meta.get('price')
        vue_data = response.xpath('//script[@id="vue-data"]/text()').extract_first()
        data_json = json.loads(vue_data)
        latitude = data_json['accommodation']['Latitude'] 
        longitude = data_json['accommodation']['Longitude']
        zipcode = data_json['accommodation']['RefPlaceCode']
        city = data_json['accommodation']['region']   
        mainregionname = data_json['accommodation']['mainRegionName'] 
        address = mainregionname + ' ' + zipcode + ' ' + city   
        if 'BedRoomCount' in data_json['accommodation']:
            room_count = data_json['accommodation']['BedRoomCount']
        else:
            room_count = ''
        if 'BathRoomCount' in data_json['accommodation']:
            bathrooms = data_json['accommodation']['BathRoomCount'] 
        else:
            bathrooms = ''
        external_id = data_json['accommodation']['AccommodationCode']  
        images_text = re.search(r'\:images=\'(.*?)\'', response.text).group(1)
        images = re.sub(r'\\x[0-9A-Z]', '', images_text)
        images_json = json.loads(images)
        images = []
        for image_json in images_json:  
            img_code = image_json['accommodationCode']
            img_id = image_json['id']  
            image = 'https://images.interhome.com/{}/partner-medium/{}'.format(img_code, img_id)
            images.append(image)
        item_loader = ListingLoader(response=response)
        item_loader.add_value('property_type', property_type)
        item_loader.add_xpath('title', '//title/text()')
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('external_link', external_link)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        item_loader.add_xpath('description', '//div[@class="content"]/p//text()')
        item_loader.add_value('rent_string', rent_string)
        item_loader.add_value('images', images)
        item_loader.add_value('latitude', str(latitude))
        item_loader.add_value('longitude', str(longitude))
        if room_count: 
            item_loader.add_value('room_count', str(room_count))
        if bathrooms:
            item_loader.add_value('bathroom_count', str(bathrooms))
        attributes = response.xpath(
            "//accommodation-attributes"
        ).get()        
        if attributes:
            feature = attributes.split("attributes='")[1].split("'")[0]
            if "parking" in feature or "garage" in feature:
                item_loader.add_value('parking', True)
            if "balcony" in feature:
                item_loader.add_value('balcony',True)
            if "dishwasher" in feature:
                item_loader.add_value('dishwasher',True)
            if "washingmachine" in feature:
                item_loader.add_value('washing_machine', True)
            if "terrace" in feature:
                item_loader.add_value('terrace', True)
            if "pool" in feature:
                item_loader.add_value('swimming_pool', True)
            if "pets" in feature:
                if "no_pets_allowed" in feature:
                    item_loader.add_value('pets_allowed', False)
                elif "pets" in feature:
                    item_loader.add_value('pets_allowed', True)
               
        item_loader.add_value('landlord_name', 'Interhome')
        item_loader.add_value('landlord_email', 'info@interhome.com')
        item_loader.add_value('landlord_phone', '+41438109126')
        item_loader.add_value("external_source", "Interhome_PySpider_belgium_nl")
        yield item_loader.load_item() 
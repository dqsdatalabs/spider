# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek
import re
import js2xml
import lxml.etree
import scrapy
from scrapy import Selector
from ..helper import extract_number_only, extract_rent_currency, remove_white_spaces,format_date
from ..loaders import ListingLoader
from math import ceil

class RulleauImmobilierFrSpider(scrapy.Spider):
    name = 'rulleau_immobilier_fr'
    allowed_domains = ['www.rulleau-immobilier.fr']
    start_urls = ['https://www.rulleau-immobilier.fr']
    execution_type = 'testing'
    country = 'france'
    locale = 'fr'
    external_source = 'Rulleau_Immobilier_PySpider_france'
    thousand_separator=' '
    scale_separator=','
    position = 0
    custom_settings = { 
         
        "PROXY_TR_ON": True,
        "CONCURRENT_REQUESTS" : 4,
        "AUTOTHROTTLE_ENABLED": True,
        "AUTOTHROTTLE_START_DELAY": .5,
        "AUTOTHROTTLE_MAX_DELAY": 2,
        "RETRY_TIMES": 3,
        "DOWNLOAD_DELAY": 1,

    }
    def start_requests(self):     
        start_urls = ["https://www.rulleau-immobilier.fr/a-louer/1"]   
        for url in start_urls:
            yield scrapy.Request(
                url=url,
                callback=self.parse,
                )

    def parse(self, response, **kwargs):
        listings = response.xpath('//div[@class="card col"]')
        
        for property_item in listings:
            title = property_item.xpath('.//h1/text()').extract_first()
            external_link = f"https://www.rulleau-immobilier.fr{property_item.xpath('.//a/@href').extract_first()}"

            yield scrapy.Request(
                url = external_link,
                callback=self.get_property_details,
                meta={
                    'external_link' : external_link,
                    'title' : title
                    })

    def get_property_details(self, response):

        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('external_link'))
        item_loader.add_value("external_source", self.external_source)
        external_id = response.xpath('.//h2/text()[4]').extract_first()
        item_loader.add_value('external_id', remove_white_spaces(external_id))
        item_loader.add_value('title', response.meta.get('title'))
        images = response.xpath('.//ul[contains(@class,"imageGallery")]/li/img/@src').extract()
        images = [f'https:{img}' for img in images]
        item_loader.add_value('images', images)
        item_loader.add_xpath('description', './/p[@itemprop="description"]/text()')
        rent_string = response.xpath('.//th[contains(text(),"Loyer")]/../th[2]/text()').extract_first()
        item_loader.add_value('rent_string',rent_string)
        zipcode = response.xpath('.//th[contains(text(),"postal")]/../th[2]/text()').extract_first()
        if zipcode:
            item_loader.add_value('zipcode', zipcode)        
        city = response.xpath('.//th[contains(text(),"Ville")]/../th[2]/text()').extract_first()
        if city:
            item_loader.add_value('city', city)
        square_meters = response.xpath('.//th[contains(text(),"habitable")]/../th[2]/text()').extract_first()
        if square_meters:
            square_meters = str(int(ceil(float(extract_number_only(square_meters,thousand_separator=' ',scale_separator=',')))))
            item_loader.add_value('square_meters', square_meters)
        room_count = response.xpath('.//th[contains(text(),"chambre")]/../th[2]/text()').extract_first()
        if room_count:
            item_loader.add_value('room_count', room_count)
        elevator = response.xpath('.//th[contains(text(),"Ascenseur")]/../th[2]/text()').extract_first()
        if elevator:
            if elevator == 'OUI':
                item_loader.add_value('elevator', True)
            elif elevator == 'NON':
                item_loader.add_value('elevator', False)
        furnished = response.xpath('.//th[contains(text(),"Meublé")]/../th[2]/text()').extract_first()
        if furnished:
            if furnished == 'OUI':
                item_loader.add_value('furnished', True)
        
        bathroom_count = response.xpath('.//th[contains(text(),"bains")]/../th[2]/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count)
        # https://www.rulleau-immobilier.fr/4446-cavignac.html
        terrace = response.xpath('.//th[contains(text(),"Terrasse")]/../th[2]/text()').extract_first()
        if terrace:
            if terrace == 'OUI':
                item_loader.add_value('terrace', True)
            elif terrace == 'NON':
                item_loader.add_value('terrace', False)
        balcony = response.xpath('.//th[contains(text(),"Balcon")]/../th[2]/text()').extract_first()
        if balcony:
            if balcony == 'OUI':
                item_loader.add_value('balcony', True)
            elif balcony == 'NON':
                item_loader.add_value('balcony', False)
        # https://www.rulleau-immobilier.fr/4446-cavignac.html
        parking = response.xpath('.//th[contains(text(),"garage")]/../th[2]/text()').extract_first()
        if parking:
            if int(parking)>0:
                item_loader.add_value('parking', True)
        deposit = response.xpath('.//th[contains(text(),"garantie")]/../th[2]/text()').extract_first()
        if deposit:
            item_loader.add_value('deposit', deposit)
 
        item_loader.add_value('address', item_loader.get_output_value('city')+ ', ' +item_loader.get_output_value('zipcode'))
        javascript = response.xpath('(.//*[contains(text(),"lng")])/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            selector = Selector(text=xml)
            latitude = selector.xpath('.//property[@name="lat"]/number/@value').get()
            longitude = selector.xpath('.//property[@name="lng"]/number/@value').get()
            item_loader.add_value('latitude',latitude)
            item_loader.add_value('longitude',longitude)

        months = {'janvier' : 'January',
                'février' : 'February',
                'mars' : 'March',
                'avril' : 'April',
                'mai' : 'May',
                'juin' : 'June',
                'juillet' : 'July',
                'aout' : 'August',
                'septembre' : 'September',
                'octobre' : 'October',
                'novembre' : 'November',
                'décembre' : 'December'}

        availability = re.findall(r'\d{2}/\d{2}/\d{4}', item_loader.get_output_value('description'))
        if availability:
            item_loader.add_value('available_date', format_date(availability[0], date_format='%d/%m/%Y'))
        #Eg: https://www.rulleau-immobilier.fr/4446-cavignac.html - 15 OCTOBRE 2020
        elif re.search(r'(\d{2}) (\w+) (\d{4})', item_loader.get_output_value('description').lower()):
            availability = re.findall(r'(\d{2}) (\w+) (\d{4})', item_loader.get_output_value('description').lower())
            date, month, year = availability[0]

            item_loader.add_value('available_date', format_date(date+months[month]+year, date_format='%d%B%Y'))
        apartment_types = ["appartement", "apartment", "flat",
                           "penthouse", "duplex", "triplex"]
        house_types = ['mobile home','park home','character property',
                'chalet', 'bungalow', 'maison', 'house', 'home', ' villa ',
                'holiday complex', 'cottage', 'semi-detached']
        studio_types = ["studio"]
        if any (i in  item_loader.get_output_value('title').lower() or i in item_loader.get_output_value('description').lower()  for i in studio_types):
            item_loader.add_value('property_type','studio')
        elif any (i in item_loader.get_output_value('title').lower() or i in item_loader.get_output_value('description').lower() for i in apartment_types):
            item_loader.add_value('property_type','apartment')
        elif any (i in item_loader.get_output_value('title').lower() or i in item_loader.get_output_value('description').lower() for i in house_types):
            item_loader.add_value('property_type','house')
        else:
            print(response.url)
                
        item_loader.add_value('landlord_name','Rulleau Immobilier')
        phone = re.findall(r'(?:\d{2}\.){4}\d{2}', item_loader.get_output_value('description'))
        if phone:
            landlord_phone = ' '.join(phone[0].split('.'))
            item_loader.add_value('landlord_phone', landlord_phone)
        else:    
            item_loader.add_value('landlord_phone', '05 57 68 93 69')
        self.position+=1
        item_loader.add_value('position',self.position)
        
        yield item_loader.load_item()
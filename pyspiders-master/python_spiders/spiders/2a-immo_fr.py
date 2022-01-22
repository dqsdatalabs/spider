# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..items import ListingItem
from ..helper import currency_parser, extract_number_only, remove_white_spaces, remove_unicode_char,format_date
import json,re

def getSqureMtr(text):
    try:
        list_text = re.findall(r'\d+',text)

        if len(list_text) == 3:
            output = float(list_text[0]+list_text[1])
        elif len(list_text) == 2:
            output = float(list_text[0]+list_text[1])
        elif len(list_text) == 1:
            output = int(list_text[0])
        else:
            output=0

        return int(output)
    except: pass
def num_there(s):
    return any(i.isdigit() for i in s)

# def getAddress(lat,lng):
#     coordinates = str(lat)+","+str(lng)
#     location = geolocator.reverse(coordinates)
#     return location.address

class HenroimmoSpider(scrapy.Spider):
    name = '2aimmo_fr_PySpider_france_fr'
    allowed_domains = ['2a-immo.fr']
    start_urls = ['https://www.2a-immo.fr/']
    execution_type = 'testing'
    country = 'france'
    locale ='fr'
    external_source = '2aimmo_fr_PySpider_france_fr'
    def start_requests(self):
        start_urls = [
            {'url': 'https://www.2a-immo.fr/location/appartement--maison?page={}'}
        ]
        for url in start_urls:
            for i in range(0,12):
                yield scrapy.Request(url=url.get('url').format(i),
                                    callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath("//article[contains(@id,'node-')]//h2/a/@href").extract()
        for property_item in listings:
            property_item = 'https://www.2a-immo.fr'+property_item
            yield scrapy.Request(
                url=property_item,
                callback=self.get_details
            )
    
    def get_details(self, response):
        item = ListingItem()
        item['external_source'] = self.external_source
        item['external_link'] = response.url
        property_tp = response.xpath("//div[@class='container']//h1/text()").extract_first()
        if 'Appartement' in property_tp:
            item['property_type'] = 'apartment'
        else:
            item['property_type'] = 'house'    
        
        item['title'] = response.xpath("//div[@class='col-12 col-lg-8']/h1/text()").extract_first()
    
        rent = "".join(response.xpath("//div[@class='price text-center text-lg-right']//text()").extract())
        if rent:
            rent = rent.replace(" ","").split('€')[0].strip()
            item['rent'] = rent
        
        item['currency'] = 'EUR'
            
        address = response.xpath("//meta[contains(@property,'og:street_address')]/@content").get()
        item['address'] = address
        city = response.xpath("//meta[contains(@property,'og:locality')]/@content").get()
        if city:
            item['city'] = city
        zipcode = response.xpath("//meta[contains(@property,'og:postal_code')]/@content").get()
        if zipcode:
            item['zipcode'] = zipcode
               
        room_count = "".join(response.xpath("//div[@class='info'][contains(.,'pièces ')]/text()[not(contains(.,'N.C.'))]").extract())
        if room_count:
            room_count = room_count.split(':')[-1].replace("\n","").strip()
            item['room_count'] = int(room_count)
        
        description = ''.join(response.xpath("//div[@class='col-12 col-lg-8 mb-50 mb-lg-0']/p/text()").extract())
        item['description'] = remove_white_spaces(description)
        if 'parking' in description:
            item['parking'] = True
        
        sq_met =  "".join(response.xpath("(//span[@class='info surface']/text())[1]").extract())
        if sq_met:
            item['square_meters'] = sq_met.split(' ')[0]
        
        deposit = "".join(response.xpath("//div[@class='info'][contains(.,'Dépôt')]/text()").extract())
        if deposit:
            item['deposit'] = deposit.replace(" ","").split('€')[0].strip()
        
        charges = "".join(response.xpath("//div[@class='info'][contains(.,'Charges')]/text()").extract())
        if charges:
            item['utilities'] = charges.split('€')[0].strip()
        
        external_id = "".join(response.xpath("//div[@class='info'][contains(.,'Référence')]/text()").extract())
        if external_id:
            item['external_id'] = external_id.split(':')[-1].strip()

        item['latitude'] = response.xpath("//meta[contains(@property,'latitude')]/@content").get()
        item['longitude'] = response.xpath("//meta[contains(@property,'longitude')]/@content").get()
       
        available = response.xpath("//div[@class='info'][contains(.,'Disponibilité')]/text()").extract()
        if num_there(available):
            item['available_date'] = format_date(available)
        
        images = [x for x in response.xpath("//div[contains(@class,'main-slider')]/div//img/@src").getall()]
        if images:
            item['images'] = images
            item['external_images_count'] = len(images)
        else:
            images = [x for x in response.xpath("//div[contains(@class,'slider')]/img/@src").getall()]
            if images:
                item['images'] = images

        landlord_name = response.xpath("(//div[contains(@class,'taxonomy-term--agency__field--name')]/text())[1]").get()
        if landlord_name:
            item['landlord_name'] = landlord_name
        landlord_phone = response.xpath("(//div[contains(@class,'taxonomy-term--agency__field--phone')]//div//text())[2]").get()
        if landlord_phone:
            item['landlord_phone'] = landlord_phone
        item['landlord_email'] = '2a-immo@2a-immo.fr'
        
        yield item

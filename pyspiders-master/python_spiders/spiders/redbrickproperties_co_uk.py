# -*- coding: utf-8 -*-
# Author: Gabriel Francis
# Team: Sabertooth

import json
import re

import scrapy
from geopy.geocoders import Nominatim

from ..loaders import ListingLoader
from ..user_agents import random_user_agent


class RedbrickpropertiesSpider(scrapy.Spider):
    name = 'redbrickproperties_co_uk'
    allowed_domains = ['www.redbrickproperties.co.uk']
    start_urls = ['http://www.redbrickproperties.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        start_urls = ['https://www.redbrickproperties.co.uk/api/2.0/properties/as-public?filter=%7B%22paginate%22:%7B%22start%22:0,%22li'\
                      'mit%22:36%7D,%22order%22:[%7B%22field%22:%22instructedDate%22,%22asc%22:false%7D,%7B%22field%22:%22property_reference%22,%22'\
                      'asc%22:false%7D],%22fields%22:%7B%22property_deleted%22:null,%22property_listed%22:1,%22bullet20%22:%7B%22strings":["1","2","3'\
                      '"]%7D%7D,%22query%22:%7B%22value%22:%22%22,%22fields%22:[%22street%22,%22district%22,%22town%22]%7D%7D']
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url,
                                       'listing_count': -1})

    def parse(self, response, **kwargs):
        response_json = json.loads(response.text)['data']['properties']
        if response.meta.get('listing_count') < len(response_json):
            current_url = response.meta['request_url']
            current_page = re.findall(r"(?<=limit%22:)\d+", current_url)[0]
            next_page_url = re.sub(r"(?<=limit%22:)\d+", str(int(current_page) + 24), current_url)
            yield scrapy.Request(url=next_page_url,
                                 callback=self.parse,
                                 meta={'request_url': next_page_url,
                                       'listing_count': len(response_json)})
        else:
            for item in response_json:
                property_url = 'https://www.redbrickproperties.co.uk/properties/property-details/'+str(item['property_reference'])
                url = "https://www.redbrickproperties.co.uk/api/2.0/properties/ref/" + str(item["property_reference"])
                yield scrapy.Request(url=url,
                                     callback=self.get_property_details,
                                     meta={'request_url': url,
                                           'property_url': property_url,
                                           'available_date': item['availableFrom'].split()[0],
                                           'room_count': item['bedrooms'],
                                           'bathroom_count': item['bathrooms'],
                                           'city': item['town'],
                                           'zipcode': item['postcode'],
                                           'external_id': item['property_reference'],
                                           'property_type': item['property_type'],
                                           'address': item['street']+', '+item['district']+', '+item['town']+', '+item['county']+', '+item['postcode']+', '+item['country']
                                           }
                                     )

    def get_property_details(self, response):
        temp_json = json.loads(response.body)
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_link", response.meta.get('property_url'))
        item_loader.add_value("external_id", str(response.meta.get('external_id')))
        item_loader.add_value("external_source", "Redbrickproperties_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_xpath("title", './/div[@class="property-details"]/h1/text()')
        
        if response.meta.get('property_type').lower() in ['house']:
            item_loader.add_value("property_type", 'house')
        elif response.meta.get('property_type').lower() in ['flat']:
            item_loader.add_value("property_type", 'apartment')
        else:
            return
        item_loader.add_value("available_date", response.meta.get('available_date'))
        item_loader.add_value("city", response.meta.get('city'))
        item_loader.add_value("zipcode", response.meta.get('zipcode'))

        # rent string
        if temp_json["data"]["property"].get("numeric_price"):
            item_loader.add_value("rent_string", '£ '+str(temp_json["data"]["property"]["numeric_price"]))

        item_loader.add_value("room_count", str(response.meta.get('room_count')))
        item_loader.add_value("bathroom_count", str(response.meta.get('bathroom_count')))
        # item_loader.add_xpath("description", './/p/text()')
        item_loader.add_value('description', temp_json["data"]["property"]["main_advert"])

        # item_loader.add_xpath("images", './/noscript//img[contains(@src,"images-property")]/@src')
        images = [l_i["picture_url"] for l_i in temp_json["data"]["property"]["property_pictures"]]
        item_loader.add_value('images', images)
        item_loader.add_value("landlord_phone", '0113 230 5552')
        item_loader.add_value("landlord_name", 'Redbrick Properties')

        if temp_json["data"]["property"].get("furnished"):
            furnished = temp_json["data"]["property"]["furnished"]
            if furnished.lower() == "furnished":
                item_loader.add_value("furnished", True)
            else:
                item_loader.add_value("furnished", False)

        parking = response.xpath('.//li[contains(text(),"parking") or contains(text(),"Parking")]/text()').extract_first()
        if parking:
            if 'no' not in parking.lower():
                item_loader.add_value('parking', True)
            else:
                item_loader.add_value('parking', False)

        terrace = response.xpath('.//li[contains(text(),"Terrace") or contains(text(),"terrace")]/text()').extract_first()
        if terrace:
            if 'no' not in terrace.lower():
                item_loader.add_value('terrace', True)
            else:
                item_loader.add_value('terrace', False)

        item_loader.add_value("address", response.meta.get('address'))
        item_loader.add_value('latitude', temp_json["data"]["property"]["latitude"])
        item_loader.add_value('longitude', temp_json["data"]["property"]["longitude"])

        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()

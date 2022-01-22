# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib
from ..loaders import ListingLoader
from python_spiders.helper import extract_rent_currency, remove_white_spaces
import re

class FitpropertyComSpider(scrapy.Spider):
    name = "fitproperty_com"
    allowed_domains = ["fitproperty.com"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    api_url = 'https://www.fitproperty.com/properties/search'
    params = {'search-type': '',
              'region': '',
              'bedrooms': '',
              'property_type': '',
              'price_from_student': '0',
              'price_to_student': '9000',
              'price_from_residential': '0',
              'price_to_residential': '9000'}
    #listing_agreed=[]
    position = 0
    
    def start_requests(self):
        start_urls = [
                        {'search-type': 'student',
                         'property_type': "Apartment",
                         'propertytype': 'apartment'},
                        {'search-type': 'student',
                         'property_type': "House",
                         'propertytype': 'house'},
                        {'search-type': 'student',
                         'property_type': "Penthouse",
                         'propertytype': 'apartment'},
                        {'search-type': 'residential',
                         'property_type': "Apartment",
                         'propertytype': 'apartment'},
                        {'search-type': 'residential',
                         'property_type': "House",
                         'propertytype': 'house'},
                        {'search-type': 'residential',
                         'property_type': "Penthouse",
                         'propertytype': 'apartment'},
                    ]
        
        for url in start_urls:
            params1 = copy.deepcopy(self.params)
            params1["search-type"] = url["search-type"]
            params1["property_type"] = url["property_type"]
            yield scrapy.FormRequest(url=self.api_url + "?" + urllib.parse.urlencode(params1),
                          callback=self.parse,
                          formdata = {'params': urllib.parse.urlencode(params1)},
                          meta={'request_url': self.api_url + "?" + urllib.parse.urlencode(params1),
                                'params': params1,
                                'property_type': url.get('propertytype')
                                }
                          )
                
    def parse(self, response, **kwargs):
        listing = response.xpath('.//*[@class="button primary arrow"]/@href').extract()
        for property_url in listing:
            yield scrapy.Request(
                url=response.urljoin(property_url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(property_url),
                      "property_type": response.meta["property_type"]}
            )
            
        if len(response.xpath('.//*[@class="button primary arrow"]')) > 0:
            params1 = copy.deepcopy(response.meta["params"])
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.FormRequest(
                url=self.api_url + "?" + urllib.parse.urlencode(params1),
                callback=self.parse,
                formdata = {'params': urllib.parse.urlencode(params1)},
                meta={'request_url': next_page_url,
                      'params': params1,
                      "property_type": response.meta["property_type"]}
            )
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        external_link = response.meta.get('request_url')
        item_loader.add_value('external_link', external_link)
        property_type = response.meta.get('property_type')
        if 'studio' in external_link:
            item_loader.add_value('property_type', 'studio')
        else:
            item_loader.add_value('property_type', property_type)
        title = response.xpath('.//*[@http-equiv="Content-Type"]/following-sibling::title/text()').extract_first()
        title = title.split('|')[0]
        item_loader.add_value('title', remove_white_spaces(title))
        address_line1 = response.xpath('.//*[@id="addressLine1"]/@value').extract_first()
        if address_line1 and len(address_line1) > 0:
            address_line1 = remove_white_spaces(address_line1)
        address_line2 = response.xpath('.//*[@id="addressLine2"]/@value').extract_first()
        if address_line2 and len(address_line2) > 0:
            address_line2 = remove_white_spaces(address_line2)
        city = response.xpath('.//*[@id="town"]/@value').extract_first()
        if city and len(city) > 0:
            city = remove_white_spaces(city)
        zipcode = response.xpath('.//*[@id="postcode"]/@value').extract_first()
        if zipcode and len(zipcode) > 0:
            zipcode = remove_white_spaces(zipcode)
        address_list = [address_line1, address_line2, city, zipcode]
        address = ', '.join(list(filter(lambda x:x!=None and len(x) > 0, address_list)))
        item_loader.add_value('address', address)
        if city:
            item_loader.add_value('city', remove_white_spaces(city))
        if zipcode:
            item_loader.add_value('zipcode', remove_white_spaces(zipcode))
        room_count = response.xpath('.//*[@class="price"]/following-sibling::div/p/text()').extract_first()
        room_search = "".join(response.xpath('//h2/text()').getall())
        if 'One' in room_count or 'One' in room_search:
            item_loader.add_value('room_count', "1")
        elif 'Individual' in room_count or 'Individual' in room_search:
            item_loader.add_value('room_count', "1")   
        elif room_count:
            room = re.search(r'\d+(?=.*bed)', room_count.lower())
            if room:
                item_loader.add_value('room_count', remove_white_spaces(room.group()))
        
        
        
        description = response.xpath('.//*[@class="primary formatting"]//p/text()').extract()
        desc = ' '.join(description[:-1])
        item_loader.add_value('description', desc)
        item_loader.add_xpath('images','.//*[@class="main-image"]//img/@src')
        rent_string = response.xpath('.//*[@class="price"]//b/text()').extract_first()
        period = response.xpath('.//*[@class="price"]//em//text()').extract()
        period = ''.join(period)
        if period and any(word in period.lower() for word in ['week', 'pw', 'pppw', 'pcw']):
            rent = rent_string.replace("£","").strip()
            item_loader.add_value("rent", int(float(rent))*4)
            item_loader.add_value("currency", "GBP")
        elif period and any(word in period.lower() for word in ['month', 'pm', 'pcm']):
            rent = rent_string.replace("£","").strip()
            item_loader.add_value('rent', rent)
            item_loader.add_value("currency", "GBP")
        else:
            item_loader.add_value('rent_string', rent_string)
        furnish_type = response.xpath('.//*[@class="furnished"]/text()').extract_first()
        if furnish_type:
            if re.search(r'un[^\w]*furnish', remove_white_spaces(furnish_type).lower()):
                item_loader.add_value('furnished', False)
            elif re.search(r'furnish', remove_white_spaces(furnish_type).lower()):
                item_loader.add_value('furnished', True)
        item_loader.add_xpath('latitude', './/*[@id="latitude"]/@value')
        item_loader.add_xpath('longitude', './/*[@id="longitude"]/@value')
        item_loader.add_value("external_source", "Fitproperty_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('landlord_name', 'Fit Property')
        item_loader.add_value('landlord_phone', '0114 272 5773')
        item_loader.add_value('landlord_email', 'hello@fitproperty.com')
        self.position += 1
        item_loader.add_value('position', self.position)
        yield item_loader.load_item()
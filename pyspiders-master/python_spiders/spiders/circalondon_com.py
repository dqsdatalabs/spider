# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import remove_unicode_char,convert_string_to_numeric, extract_rent_currency, format_date, extract_number_only
import json
import re


class CircaLondonSpider(scrapy.Spider):
    name = "circalondon_com"
    allowed_domains = ['circalondon.com']
    start_urls = ['https://www.circalondon.com/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    position = 0
    thousand_separator = ','
    scale_separator = '.'
    api_url = 'https://www.circalondon.com/api/set/results/map-data-cords'
    params = {
        'sortorder': 'price-desc',
        'RPP': '12',
        'OrganisationId': '7d3c0a96-c5a7-420f-8064-f8adc9f4bf5b',
        'WebdadiSubTypeName': 'Rentals',
        'Status': '{2a50fde6-8f09-4d01-9514-7a856e206d04},{e9617465-c405-4b6a-abc9-fdbfc499145c},{59c95297-2dca-4b55-9c10-220a8d1a5bed}',
        'includeSoldButton': 'true',
        'incsold': 'true',
    }
     
    def start_requests(self):
        start_url = ['https://www.circalondon.com/let/property-to-let']
        for url in start_url:
            yield scrapy.FormRequest(url=self.api_url,
                                     callback=self.parse,
                                     method='POST',
                                     formdata=self.params,
                                     meta={'request_url': url})

    def parse(self, response, **kwargs):
        temp_json = json.loads(response.body)
        listings = json.loads(temp_json['properties'])
        for property_item in listings:
            url = 'https://www.circalondon.com'+property_item['url']
            
            yield scrapy.Request(
                    url=url,
                    callback=self.get_property_details,
                    meta={'request_url': url,
                          'latitude': property_item['latitude'],
                          'longitude': property_item['longitude'],
                          'property_type': property_item['propertytypename'],
                          'external_id': property_item['internalcode'],
                          'room_count': property_item['numberbedrooms'],
                          'zipcode': property_item['postcode'],
                          'address': property_item['label'],
                          })

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Circalondon_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('external_id'))
        
        if "apartment" in response.meta.get('property_type').lower() or "flat" in response.meta.get('property_type').lower():
            item_loader.add_value("property_type", "apartment")
        else: return

        address = response.meta.get('address')
        city = address.split(', ')[-2]
        item_loader.add_value('address', address)
        item_loader.add_value('title', address)
        item_loader.add_value('city', city)

        item_loader.add_value('room_count', response.meta.get('room_count'))
        item_loader.add_value('zipcode', response.meta.get('zipcode'))
        item_loader.add_value('latitude', response.meta.get('latitude'))
        item_loader.add_value('longitude', response.meta.get('longitude'))
        item_loader.add_xpath('bathroom_count', '//li[@class="FeaturedProperty__list-stats-item"][2]/span/text()')
        item_loader.add_xpath('images', './/div[contains(@class,"owl-image lazyload")]/@data-bg')

        rent = response.xpath('.//h2[contains(text(),"per month")]').extract_first()
        if not rent:
            rent = response.xpath('.//h2/span[@class="nativecurrencyvalue"]/following-sibling::text()').extract_first()
        rent = re.search(r'(?<=\()(.+)(?=\))', rent)
        if rent:
            rent_string = '£' + str(extract_number_only(rent.group(), thousand_separator=','))
            item_loader.add_value('rent_string', rent_string)

        item_loader.add_xpath('description', './/section[@id="description"]/p/text()')
        item_loader.add_value('landlord_name', 'Circa London')
        item_loader.add_xpath('landlord_phone', './/a[contains(@href,"tel:")]/text()')
        item_loader.add_xpath('landlord_email', './/a[contains(@href,"mailto:")]/text()')
        item_loader.add_xpath('floor_plan_images', './/h2/../..//img[@title="floorplan"]/@data-src')

        features = ", ".join(response.xpath('.//div[@id="collapseOne"]//li/text()').extract())

        if "parking" in features.lower():
            item_loader.add_value('parking', True)
        
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        if "swimming pool" in features.lower():
            item_loader.add_value('swimming_pool', True)

        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)
        
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)

        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)
        
        if "furnished" in features or "Furnished" in features:
            if "unfurnished" in features.lower():
                item_loader.add_value("furnished", False)
            else:
                item_loader.add_value("furnished", True)
        if "sq.ft" in features:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq.ft)",features.replace(",","").replace("(",""))
            if unit_pattern:
                square_title=unit_pattern[0][0]
                sqm = str(int(float(square_title) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
        
        self.position += 1
        item_loader.add_value('position', self.position)
        
        status = response.xpath("//h2[contains(@class,'mobile-left')]/text()").get()
        if status and "let agreed" in status.lower():
            return
        yield item_loader.load_item()

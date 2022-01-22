# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib
from ..loaders import ListingLoader
from ..helper import remove_white_spaces, extract_rent_currency, convert_string_to_numeric, extract_number_only
import re
import math


class CleverpropertySpider(scrapy.Spider):
    name = "cleverproperty_com"
    allowed_domains = ["cleverproperty.com"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    external_source = "Cleverproperty_PySpider_united_kingdom_en"
    api_url = 'https://www.cleverproperty.com/'
    params = {'kwa[]': '',
              'type[]': '',
              'id': 4116,
              'do': 'search',
              'for': 2,
              'maxbeds': 9999,
              'minprice': 0,
              'maxprice': 99999999999,
              'order': 2,
              'page': 0,
              'Search': ''}
    # listing_new=[]
    position = 0

    def start_requests(self):
        start_urls = [
            {'type[]': 8,
             "property_type": "apartment"
             },
            {
                'type[]': 6,
                "property_type": "house"
            }
        ]
        for url in start_urls:
            params1 = copy.deepcopy(self.params)
            params1["type[]"] = url["type[]"]
            yield scrapy.Request(url=self.api_url + "?" + urllib.parse.urlencode(params1),
                                 callback=self.parse,
                                 meta={'request_url': self.api_url + "?" + urllib.parse.urlencode(params1),
                                       'params': params1,
                                       'property_type': url.get("property_type")})
                
    def parse(self, response, **kwargs):
        listing = response.xpath('.//*[@class="secondary"]/@href').extract()
        for property_url in listing:
            yield scrapy.Request(
                    url=response.urljoin(property_url),
                    callback=self.get_property_details,
                    meta={'request_url': response.urljoin(property_url),
                          "property_type": response.meta["property_type"]}
                )

        if len(response.xpath('.//*[@class="secondary"]')) > 0:
            current_page = response.meta["params"]["page"]
            params1 = copy.deepcopy(response.meta["params"])
            params1["page"] = current_page + 1
            next_page_url = self.api_url + "?" + urllib.parse.urlencode(params1)
            yield scrapy.Request(
                url=self.api_url + "?" + urllib.parse.urlencode(params1),
                callback=self.parse,
                meta={'request_url': next_page_url,
                      'params': params1,
                      "property_type": response.meta["property_type"]})
            
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        header = response.xpath('.//*[@class="pull-left status secondary"]/text()').extract()
        headers = ''.join(header)

        # Checks if let agreed and proceeds ahead if not
        if 'to let' not in headers.lower() and 'short let' not in headers.lower():
            return

        item_loader.add_xpath('title', './/*[@name="Description"]/@content')
        item_loader.add_value('external_link', response.meta.get('request_url'))
        property_type = "".join(response.xpath("//h2//text()[contains(.,'Studio')]").getall())
        if property_type:
            item_loader.add_value("property_type", "studio")
        else:
            item_loader.add_value('property_type', response.meta.get('property_type'))

        rent = "".join(response.xpath("//h2//text()").getall())
        if rent:
            price = rent.split("£")[1].split(".")[0]
            if "pw" in rent.lower():
                price = int(price)*4
            item_loader.add_value("rent", price)
        item_loader.add_value("currency", "GBP")

        external_id = response.xpath('.//*[@class="pull-right text-shadow"]/text()').extract_first()
        item_loader.add_value('external_id', extract_number_only(external_id, thousand_separator=',', scale_separator='.'))

        item_loader.add_xpath('images', './/img[@u="image"]/@src')

        floor_plans = response.xpath('.//*[contains(@src,"floorplan")]/@src').extract()
        if len(floor_plans) > 0:
            item_loader.add_value('floor_plan_images', [response.urljoin(floor_plan) for floor_plan in floor_plans])

        room_count = response.xpath('//*[contains(@src,"beds")]/following-sibling::text()').extract_first()
        if room_count and remove_white_spaces(room_count.lower()) == 'studio':
            item_loader.add_value('room_count', '1')
        elif room_count and remove_white_spaces(room_count) != '0':
            item_loader.add_value('room_count', remove_white_spaces(room_count))

        bathroom_count = response.xpath('//*[contains(@src,"baths")]/following-sibling::text()').extract_first()
        if bathroom_count and extract_number_only(bathroom_count) != '0':
            item_loader.add_value('bathroom_count', extract_number_only(bathroom_count))
        
        # https://www.cleverproperty.com/property-search~action=detail,pid=527
        item_loader.add_xpath('description', './/h4[contains(text(),"Overview")]/following-sibling::div/text()')
        # https://www.cleverproperty.com/property-search~action=detail,pid=50
        item_loader.add_xpath('description', './/h4[contains(text(),"Overview")]/following-sibling::p/text()')

        area = response.xpath('.//*[@id="living-space"]/text()').extract_first()
        units = response.xpath('.//*[@id="living-space"]/following-sibling::text()').extract_first()
        if area and math.ceil(convert_string_to_numeric(area, CleverpropertySpider)) != 0:
            if "sqft" in units:
                square_meter = convert_string_to_numeric(area, CleverpropertySpider)*0.09290
                item_loader.add_value('square_meters', str(math.ceil(square_meter)))
            elif "sqm" in units:
                item_loader.add_value('square_meters', str(math.ceil(area)))

        address_link = response.xpath('.//iframe[contains(@src,"maps")]/@src').extract_first()
        if address_link:
            address = re.search(r'(?<=&q=.)\w+.*(?=&ie)', address_link)
            if address:
                item_loader.add_value('address', address.group())
                item_loader.add_value('city', address.group().split(', ')[-3])
                item_loader.add_value('zipcode', address.group().split(', ')[-2])

        features = response.xpath('.//ul[contains(@class,"features-list")]/text()').extract_first()
        if features:
            # https://www.cleverproperty.com/property-search~action=detail,pid=385
            if 'un-furnished' in features.lower() and 'furnished or unfurnished' not in features.lower():
                item_loader.add_value('furnished', False)
            elif 'furnished' in features.lower() and 'furnished or unfurnished' not in features.lower():
                item_loader.add_value('furnished', True)

            # https://www.cleverproperty.com/property-search~action=detail,pid=139
            if 'terrace' in features.lower():
                item_loader.add_value('terrace', True)

            # https://www.cleverproperty.com/property-search~action=detail,pid=139
            if 'balcony' in features.lower():
                item_loader.add_value('balcony', True)

            # https://www.cleverproperty.com/property-search~action=detail,pid=235
            if 'swimming pool' in features.lower() or 'pool' in features.lower():
                item_loader.add_value('swimming_pool', True)

            # https://www.cleverproperty.com/property-search~action=detail,pid=139
            if 'parking' in features.lower():
                item_loader.add_value('parking', True)

            if 'dishwasher' in features.lower():
                item_loader.add_value('dishwasher', True)

            # https://www.cleverproperty.com/property-search~action=detail,pid=226
            if 'lift' in features.lower() or 'elevator' in features.lower():
                item_loader.add_value('elevator', True)

            # https://www.cleverproperty.com/property-search~action=detail,pid=526
            floor = re.search(r'\d{1,2}(?=\w{0,2}\sfloor)', features.lower())
            if floor and floor.group().isdigit():
                item_loader.add_value('floor', floor.group())

        item_loader.add_value('landlord_name', 'Clever Property')
        item_loader.add_value('landlord_phone', '020 8257 8202')
        item_loader.add_value('landlord_email', ' hello@cleverproperty.com')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item()

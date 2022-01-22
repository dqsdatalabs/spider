# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date
import re


class LondonhabitatCoUkSpider(scrapy.Spider):
    name = "londonhabitat_co_uk"
    allowed_domains = ["londonhabitat.co.uk"]
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    external_source="Londonhabitat_PySpider_united_kingdom_en"

    def start_requests(self):

        start_urls = ["https://www.londonhabitat.co.uk/properties/to-let/"]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse, 
                                 meta={'request_url': url,
                                       'page': 1})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="photo"]/a/@href').extract()
        for url in listings:
            yield scrapy.Request( 
                url=response.urljoin(url),
                callback=self.get_property_details,
                meta={'request_url': response.urljoin(url),
                      'external_id': url.split('/')[1]})
                
        if len(listings) == 12:
            next_page_url = '/properties/to-let/?page=' + str(response.meta.get('page')+1)
            if next_page_url:
                yield scrapy.Request(
                        url=response.urljoin(next_page_url),
                        callback=self.parse,
                        meta={'request_url': response.urljoin(next_page_url),
                              'page': response.meta.get('page')+1})

    def get_property_details(self, response):

        available_date = response.xpath('//div[@class="available"]//text()')
        if available_date:
            available_date = available_date.extract_first().split('Available: ')[-1]
        
        description = "".join(response.xpath('//div[@class="description"]/text()').extract())
        features = ", ".join(response.xpath('//div[@class="features"]//li/text()').extract())
        
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('external_id'))
        item_loader.add_xpath('address', '//div[@class="address"]/h2/text()')
        city = response.xpath('//div[@class="address"]/h2/text()').get()
        if city:
            if "-" in city:
                city = city.split(",")[-2]
            else:
                city = city.split(",")[-1]
            item_loader.add_value("city", city)
        room_count = extract_number_only(response.xpath('.//span[@class="bedsWithTypeBeds"]/text()').extract_first())

        rent = response.xpath('//span[@class="displayprice"]//text()').get()
        if rent:
            rent_pw = response.xpath('//span[@class="displaypricequalifier"]//text()[contains(.,"pw")]').get()
            rent = rent.split("£")[1].replace(",","").strip()
            if rent_pw:
                rent = int(rent)*4
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")
        item_loader.add_xpath('title', '//div[@class="topleft"]//div[@class="address"]/text()')
        property_type = response.xpath('.//span[@class="bedsWithTypePropType"]/text()').extract_first()
        property_type_mapping = {"flat": "apartment", "maisonette": "apartment", "detached house": "house","terraced house":"house","mews house":"house","semi-house":"house"}
        if property_type:
            property_type = property_type.lower()
            for key_i in property_type_mapping:
                property_type = property_type.replace(key_i, property_type_mapping[key_i])
            if "apartment" in description:
                property_type = "apartment" 
            elif response.xpath("//div[@class='address']/text()[contains(.,'studio')]").get():
                property_type = "studio"       
            elif room_count == "1":                
                property_type = "studio"
            elif room_count == "1":                
                property_type = "apartment"
            item_loader.add_value('property_type', property_type)
        
        if room_count != 0:
            item_loader.add_value('room_count', room_count)
        elif property_type in ["studio"]:
            item_loader.add_value('room_count', "1")

        item_loader.add_value('available_date', format_date(available_date))

        if item_loader.get_output_value('address'):
            if any(ch.isdigit() for ch in item_loader.get_output_value('address').split(', ')[-1].split(' ')[-1]):
                item_loader.add_value('zipcode', item_loader.get_output_value('address').split(', ')[-1].split(' ')[-1])

        bathroom_count = extract_number_only(response.xpath('//div[@class="features"]//li[contains(text(),"bathroom") or contains(text(),"Bathroom")]/text()').extract_first())
        if bathroom_count != 0:
            item_loader.add_value('bathroom_count', bathroom_count)

        lat_long = response.xpath('//img/@onload').extract_first()
        if lat_long:
            lat_long = lat_long.split('(')[-1].split(',')
            item_loader.add_value('latitude', lat_long[0])
            item_loader.add_value('longitude', lat_long[1])

        item_loader.add_value('description', description)
        item_loader.add_xpath('images', '//div[not(@id="hiddenfloorplan")]/div[@class="propertyimagelist"]//img/@src')
        item_loader.add_xpath('floor_plan_images', '//div[@id="hiddenfloorplan"]//img/@src')
        
        # https://www.londonhabitat.co.uk/property/8395/?propInd=L&page=1&pageSize=12&orderBy=PriceSearchAmount
        if "terrace" in features.lower():
            item_loader.add_value('terrace', True)

        # https://www.londonhabitat.co.uk/property/8339/?propInd=L&page=4&pageSize=12&orderBy=PriceSearchAmount
        if "balcony" in features.lower():
            item_loader.add_value('balcony', True)
        if "washing machine" in features.lower():
            item_loader.add_value('washing_machine', True)
        if "dishwasher" in features.lower():
            item_loader.add_value('dishwasher', True)

        # https://www.londonhabitat.co.uk/property/1234/?propInd=L&page=4&pageSize=12&orderBy=PriceSearchAmount
        if "parking" in features.lower():
            item_loader.add_value('parking', True)

        # https://www.londonhabitat.co.uk/property/8395/?propInd=L&page=1&pageSize=12&orderBy=PriceSearchAmount
        if "elevator" in features.lower() or "lift" in features.lower():
            item_loader.add_value('elevator', True)

        if "furnished" in features.lower():
            if "unfurnished" in features.lower():
                item_loader.add_value('furnished', False)
            else:
                item_loader.add_value('furnished', True)
        
        item_loader.add_value('landlord_name', 'London Habitat')
        item_loader.add_value('landlord_phone', '02077944041')
        item_loader.add_value('landlord_email', 'info@londonhabitat.co.uk')

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", self.external_source)
        yield item_loader.load_item()

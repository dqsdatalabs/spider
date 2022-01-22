# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, copy, urllib
from ..loaders import ListingLoader
from ..helper import extract_number_only, format_date, remove_white_spaces
from geopy.geocoders import Nominatim
from scrapy import Request,FormRequest
import re
import json
from ..user_agents import random_user_agent


class WichesterWhiteSpider(scrapy.Spider):
    name = 'winchester_white_co_uk'
    allowed_domains = ['winchester-white.co.uk']
    start_urls = ['http://www.winchester-white.co.uk']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
 
    def start_requests(self):

        start_urls = [{
            'url': 'http://www.winchester-white.co.uk/search/apartment?filter-show=let-stc',
            'property_type': 'apartment'
            },
            {
            'url': 'http://www.winchester-white.co.uk/search/house?filter-show=let-stc',
            'property_type': 'house'
            },
            {
            'url': 'http://www.winchester-white.co.uk/search/maisonette?filter-show=let-stc',
            'property_type': 'house'
            },
            {
            'url': 'http://www.winchester-white.co.uk/search/penthouse?filter-show=let-stc',
            'property_type': 'house'
            },
            {
            'url': 'http://www.winchester-white.co.uk/search/residential-property?filter-show=let-stc',
            'property_type': 'apartment'
            },
            {
            'url': 'http://www.winchester-white.co.uk/search/studio?filter-show=let-stc',
            'property_type': 'studio'
            },
            {
            'url': 'http://www.winchester-white.co.uk/search/flat?filter-show=let-stc',
            'property_type': 'apartment'
            },
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'request_url': url.get("url"),
                                       "property_type": url.get("property_type")})
    
    def parse(self, response, **kwargs):
        listings = response.xpath('.//property-search-main/@search-data').extract_first()
        listings = [l_i["url"] for l_i in json.loads(listings)["results"]]
        for property_url in listings:
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'property_type': response.meta["property_type"]}
            )
        
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        
        rented = response.xpath("//div[@class='property-details-page-header-details-wrapper-label mb-2']/text()").extract_first()
        if rented:
            return
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_xpath('title', './/*[contains(@class,"wrapper-address")]/text()')
        item_loader.add_value('external_link', response.meta.get('request_url'))

        desc = " ".join(response.xpath("//p//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        item_loader.add_xpath('rent_string', './/div[contains(@class, "property-details-page-header-right-col")]//span[contains(text(), "p/cm")]/../text()')
        
        item_loader.add_xpath('energy_label', './/div[@class="rating rating-col"]//span[@data-label="Current"]/text()')
        item_loader.add_xpath('images', './/*[contains(@class,"page-header-gallery")]//img/@data-src')
        floor_plan_images = response.xpath('.//li[contains(text(), "Floorplan")]/@data-light-box-image-src').extract_first()
        if floor_plan_images:
            item_loader.add_value('floor_plan_images', json.loads(floor_plan_images)[0]["src"])
        
        room_count = response.xpath('.//span[contains(text(),"Bedrooms")]/../text()').extract_first()
        if room_count and room_count != '0':
            item_loader.add_value('room_count', extract_number_only(room_count))
        elif room_count and room_count == '0' and property_type.lower() == "studio":
            item_loader.add_value('room_count', '1')
        item_loader.add_xpath('bathroom_count', './/span[contains(text(),"Bathrooms")]/../text()')

        # furnished
        # https://www.winchester-white.co.uk/listing/wimbledon/long-lets-oak-hill-court
        furnished = response.xpath('.//div[contains(text(),"Furnished")]/following-sibling::div/text()').extract_first()
        if furnished and furnished == "Furnished":
            item_loader.add_value('furnished', True)
        elif furnished and furnished == "Unfurnished":
            item_loader.add_value('furnished', False)

        address = response.xpath('//*[contains(@class,"wrapper-address")]/text()').get()
        if address:
            city = address.split(",")[0]
            zipcode = address.split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            if address.count(",") > 0:
                item_loader.add_value("zipcode", zipcode.strip())

        if response.xpath('.//div[@data-map-key]').extract_first():
            latitude = response.xpath('.//div[@data-map-key]/@data-latitude').extract_first()
            longitude = response.xpath('.//div[@data-map-key]/@data-longitude').extract_first()
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

            # geolocator = Nominatim(user_agent=random_user_agent())
            # location = geolocator.reverse(", ".join([str(latitude), str(longitude)]))
            # if location:
            #     item_loader.add_value('address', location.address)
            #     if "city" in location.raw["address"]:
            #         item_loader.add_value('city', location.raw["address"]["city"])
            #     if "postcode" in location.raw["address"]:
            #         item_loader.add_value("zipcode", location.raw["address"]["postcode"])info@winchester-white.co.uk <info@winchester-white.co.uk>;
        
        # email = response.xpath('.//*[contains(@class,"body-action-email")]/a/@href').extract_first().split(':')[1]
        # item_loader.add_value('landlord_email', email)
        phone = response.xpath('.//*[contains(@class,"body-action-contact")]/a/@href').extract_first().split(':')[1]
        item_loader.add_value('landlord_phone', phone)
        item_loader.add_xpath('landlord_name', './/*[contains(@class,"body-name")]/text()')
        item_loader.add_value('landlord_email', 'info@winchester-white.co.uk')
        
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "WinchesterWhite_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()

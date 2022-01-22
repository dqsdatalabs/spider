# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy, urllib
import re
import json
import dateparser
from ..loaders import ListingLoader
from ..helper import remove_unicode_char, extract_rent_currency, format_date, extract_number_only
from datetime import date
from ..user_agents import random_user_agent


class HonestySpider(scrapy.Spider):

    name = 'honesty_be'
    allowed_domains = ['honesty.be']
    start_urls = ['https://honesty.be/']
    execution_type = 'testing'
    country = 'belgium'
    locale = 'fr'
    external_source='Honesty_PySpider_belgium_fr'
    thousand_separator = '.'
    scale_separator = ','
    position = 0

    def start_requests(self):
        start_urls = ["https://honesty.be/proxy/https://sbs.whise.eu/websiteservices/EstateService.svc/GetEstateList/proxydata/EstateServiceGetEstateListRequest=%7B%22StatusIDList%22:[1,3],%22Language%22:%22fr-BE%22,%22ShowDetails%22:1,%22Page%22:0,%22RowsPerPage%22:350,%22PutOnlineDateTime%22:true,%22PurposeIDList%22:[2,3],%22PriceRange%22:[0,100000000],%22OrderByFields%22:[%22CreateDateTime%20DESC%22]%7D"]
        for url in start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse,
                                 meta={'request_url': url})

    def parse(self, response, **kwargs):
        temp_json = json.loads(response.body)
        for property_item in temp_json["d"]["EstateList"]:
            property_url = "https://honesty.be/nos-biens/a-louer/" + str(property_item["EstateID"])
            yield scrapy.Request(
                url=property_url,
                callback=self.get_property_details,
                meta={'request_url': property_url,
                      'city': property_item["City"],
                      'external_id': property_item['EstateID'],
                      'address': property_item['Address1'],
                      'zipcode': property_item['Zip'],
                      'property_type': property_item['Category'],
                      'square_meters': property_item['Area'],
                      'floor': property_item['Floor'],
                      'available_date': property_item['AvailabilityDateTime']}
            )
    
    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('city', response.meta["city"])
        item_loader.add_value('external_id', str(response.meta['external_id']))
        item_loader.add_value('address', response.meta['address'])
        item_loader.add_value('zipcode', response.meta['zipcode'])
        
        item_loader.add_value('floor', response.meta['floor'])
        if response.meta['available_date']:
            available_date = re.findall(r"(?<=/Date\()\d+", response.meta['available_date'])[0]
            available_date = dateparser.parse(available_date).strftime("%Y-%m-%d")
            item_loader.add_value('available_date', format_date(available_date, "%Y-%m-%d"))
        if response.meta['square_meters'] and response.meta['square_meters'] > 0:
            item_loader.add_value('square_meters', str(response.meta['square_meters']))
        
        if "maison" in response.meta['property_type']:
            item_loader.add_value('property_type',"house")
        elif "appartement" in response.meta['property_type'] :
            item_loader.add_value('property_type',"apartment")
        else:return
        title = response.xpath('//h1[@class="property__title"]/text()').extract_first()
        if title:
            item_loader.add_value('title', title.replace("Honesty","").replace("honesty","").replace("HONESTY",""))
            if "parking" in title.lower():
                item_loader.add_value('parking', True)
        item_loader.add_xpath('room_count', './/div[@class="property__room"]/text()')
        item_loader.add_xpath('bathroom_count', './/div[@class="property__bath"]/text()')
        item_loader.add_xpath('rent_string', './/div[@class="property__price"]/text()')
        item_loader.add_xpath('images', './/div[@class="property__gallery"]//img[contains(@src, "/honesty/Pictures")]/@src')
        description = response.xpath('.//div[@class="property__content"]/p/text()').extract_first()
        item_loader.add_value('description', description.replace("Honesty","").replace("honesty","").replace("HONESTY",""))
        item_loader.add_xpath('landlord_phone', './/a[@class="property__form-phone"]/text()')
        item_loader.add_value('landlord_email', "info@honesty.be")
        item_loader.add_value('landlord_name', "Honesty SRL")

        item_loader.add_xpath('latitude', "substring-before(substring-after(substring-after(//div[@class='property__map-container']/@data-map,'lat'),': '),',')")
        longitude = "".join(response.xpath("substring-before(substring-after(substring-after(//div[@class='property__map-container']/@data-map,'lng'),': '),' ')").extract())
        if longitude:
            item_loader.add_value('longitude', longitude.strip())

        # https://honesty.be/nos-biens/a-louer/4184785
        if "terrasse" in description.lower():
            item_loader.add_value('terrace', True)

        # https://honesty.be/nos-biens/a-louer/4195459
      
        parking = response.xpath('.//dd[contains(text(), "Parking")]/span/text()').extract_first()
        if parking:
            if parking == "Oui":
                item_loader.add_value('parking', True)
            else:
                item_loader.add_value('parking', False)
        utilities = response.xpath("//div[@class='property__content']/p/text()[contains(.,'provisions')]").extract_first()
        if utilities:
            item_loader.add_value('utilities', utilities.split("provision")[0].split("+")[-1])

        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Honesty_PySpider_{}_{}".format(self.country, self.locale))
        return item_loader.load_item()

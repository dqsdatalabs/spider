# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
from ..helper import extract_number_only
from ..loaders import ListingLoader
import re

class MovingcityCoUkSpider(scrapy.Spider):
    name = 'movingcity_co_uk'
    allowed_domains = ['movingcity.co.uk']
    start_urls = ['https://movingcity.co.uk/']
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0
    page = 1

    def start_requests(self):

        start_urls = [
            {
                'url': 'https://www.movingcity.co.uk/property-search/apartments-to-rent-in-london/page-1?order-group=Highest%20Price%20First',
                'param': 'apartments',
                'property_type': 'apartment'},
            {
                'url': 'https://www.movingcity.co.uk/property-search/houses-to-rent-in-london/page-1?order-group=Highest%20Price%20First',
                'param': 'houses',
                'property_type': 'house'},
        ]
        for url in start_urls:
            yield scrapy.Request(url=url.get("url"),
                                 callback=self.parse,
                                 meta={'property_type': url.get('property_type'),
                                       'param': url.get('param')})

    def parse(self, response, **kwargs):
        listings = response.xpath('.//div[@class="property-item-inner"]/div/a/@href').extract()
        if listings:
            for property_item in listings:
                yield scrapy.Request(
                    url=f"https://www.movingcity.co.uk/{property_item}",
                    callback=self.get_property_details,
                    meta={'request_url': f"https://www.movingcity.co.uk/{property_item}",
                          'property_type': response.meta.get('property_type')}
                )

            self.page += 1
            yield scrapy.Request(
                url=response.urljoin(
                    f"https://www.movingcity.co.uk/property-search/{response.meta.get('param')}-to-rent-in-london/page-{self.page}?order-group=Highest%20Price%20First"),
                callback=self.parse,
                meta={'property_type': response.meta.get('property_type'),
                      'param': response.meta.get('param')}
            )

    def get_property_details(self, response):
        rented = response.xpath("//div[@class='status']/text()[.='Let Agreed']").extract_first()
        if rented:
            return
        
        item_loader = ListingLoader(response=response)
        
        item_loader.add_value('property_type', response.meta.get('property_type'))
        item_loader.add_value('external_link', response.meta.get('request_url'))
        item_loader.add_value('external_id', response.meta.get('request_url').split("/")[-1])
        item_loader.add_xpath('title', './/title/text()')
        item_loader.add_xpath('address', './/div[@class="prop_address"]/text()')
        item_loader.add_xpath('rent_string', './/span[@class="price-qualifier"]/text()')
        
        description = response.xpath('.//div[@class="property-details"]//p/text()').extract_first()
        item_loader.add_value('description', description)

        item_loader.add_xpath('images', './/div[@class="modal-body"]//img/@src')
        item_loader.add_value('landlord_name', 'Moving City')
        item_loader.add_value('landlord_phone', '020 7481 1110')
        # ex https://www.movingcity.co.uk/property-for-rent/apartment-to-rent-in-crawford-building-aldgate/2853 19th floor
        item_loader.add_xpath('floor', './/li[contains(text(),"Floor")]/text()')
        javascript = response.xpath('.//script[contains(text(), "startekDetailsMap")]/text()').extract_first()
        if javascript:
            # lxml for not and giving error Unexpected '<' at 202:1 after ';'
            # xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            lat_lng = javascript.split("startekDetailsMap($, '", 1)[-1].split("','", 2)[:2]
            item_loader.add_value('latitude', lat_lng[0])
            item_loader.add_value('longitude', lat_lng[1])
        # present as key point as li tag
        balcony = response.xpath('.//li[contains(text(),"Balcony")]').extract_first()
        if balcony:
            item_loader.add_value('balcony', True)

        furnished = response.xpath('.//li[contains(text(),"Furnished")]')
        if furnished:
            item_loader.add_value('furnished', True)

        room = response.xpath('.//div[@class="property-attributes"]//span[contains(text(),"Bedroom")]/text()').extract_first()
        if room:
            room_element = extract_number_only(room)
            if room_element:
                item_loader.add_xpath('room_count', room_element)

        bath = response.xpath('.//div[@class="property-attributes"]//span[contains(text(),"Bathroom")]/text()').extract_first()
        if bath:
            bath_element = extract_number_only(bath)
            if bath_element:
                item_loader.add_xpath('bathroom_count', bath_element)

        # some have zip in the addresses and some not
        if item_loader.get_output_value('address'):
            city_zip = item_loader.get_output_value('address').split(',')[-1].strip()
            if len(city_zip.split(" ")) == 1:   
                item_loader.add_value('zipcode', city_zip)
                city = item_loader.get_output_value('address').split(',')[-2].strip()
                if "Street" not in city and "Place" not in city:
                    item_loader.add_value('city', city)
            elif len(city_zip.split(" ")) > 1:
                zipcode = city_zip.split(" ")[-1]
                if not zipcode.isalpha():
                    item_loader.add_value('zipcode', zipcode)
                    item_loader.add_value('city', " ".join(city_zip.split(" ")[:-1]))
                else:
                    item_loader.add_value('city', city_zip)
            # if any(ch.isdigit() for ch in city_zip[-1].split()[-1]):
            #     item_loader.add_value('zipcode', city_zip[-1].split()[-1])
        features = ' '.join(response.xpath('.//ul[@class="attributes"]/li/text()').extract())

        if 'swimming pool' in features.lower():
            item_loader.add_value('swimming_pool', True)
        if features:
            unit_pattern = re.findall(r"[+-]? *((?:\d+(?:\.\d*)?|\.\d+)(?:[eE][+-]?\d+)?)\s*(Sq. Ft.|sq. ft.|Sq. ft.|sq. Ft.|sq|Sq)",features.replace(",",""))
            if unit_pattern:
                square=unit_pattern[0][0]
                sqm = str(int(float(square) * 0.09290304))
                item_loader.add_value("square_meters", sqm)
        self.position += 1
        item_loader.add_value('position', self.position)
        item_loader.add_value("external_source", "Movingcity_PySpider_{}_{}".format(self.country, self.locale))
        yield item_loader.load_item()

# -*- coding: utf-8 -*-
# Author: Pavit Kaur
# Team: Sabertooth

import scrapy
from ..loaders import ListingLoader
from scrapy import Selector
import requests
from ..helper import format_date
import js2xml
import lxml


class VenicepropertiesCoSpider(scrapy.Spider):
    name = "veniceproperties_co"
    allowed_domains = ["veniceproperties.co"]
    start_urls = (
        "http://www.veniceproperties.co/?id=29487&action=view&route=search&view=list&input=W2&jengo_radius=5&jengo_property_for=2&jengo_property_type=-1&jengo_category=1&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&daterange=undefined&longitude=#total-results-wrapper",
        "http://www.veniceproperties.co/?id=29487&action=view&route=search&view=list&input=W2&jengo_radius=5&jengo_property_for=2&jengo_property_type=-1&jengo_category=2&jengo_min_beds=0&jengo_max_beds=9999&jengo_min_price=0&jengo_max_price=99999999999&jengo_order=6&pfor_complete=&pfor_offer=&trueSearch=&searchType=postcode&latitude=&daterange=undefined&longitude=#total-results-wrapper"
    )
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    thousand_separator = ','
    scale_separator = '.'
    position = 0

    def start_requests(self):
        for url in self.start_urls:
            yield scrapy.Request(url=url,
                                 callback=self.parse)

    def parse(self, response, **kwargs):
        listings = response.xpath('//div[@class="grid-result"]//a/@href').extract()
        for property_url in listings:
            property_url = response.urljoin(property_url)
            yield scrapy.Request(url=property_url,
                                 callback=self.get_property_details,
                                 meta={'request_url':property_url}
                                 )

    def get_property_details(self, response):
        item_loader = ListingLoader(response=response)

        property_type = response.xpath('//span[contains(text(),"Type")]/following-sibling::span/strong/text()').extract_first()
        property_mapping = {"flat": "apartment"}
        if property_type:
            property_type = property_type.strip().lower()
            for key_i in property_mapping:
                property_type = property_type.replace(key_i, property_mapping[key_i])
            item_loader.add_value('property_type', property_type)

        external_id = response.meta.get('request_url').split('/')[-2]
        item_loader.add_value('external_id', external_id)
        item_loader.add_xpath('title', './/title/text()')
        item_loader.add_xpath('description', '//p[@class="description-text"]/following-sibling::p/text()')

        address = response.xpath('//span[@class="details-address"]//text()').extract()
        if address:
            address = [i.strip() for i in address if i != ", "]
            address = ", ".join(address)
            item_loader.add_value('address', address)

            zipcode = address.split(', ')[-1]
            item_loader.add_value('zipcode', zipcode)

        javascript = response.xpath('.//script[contains(text(), "prop_lat")]/text()').extract_first()
        if javascript:
            xml = lxml.etree.tostring(js2xml.parse(javascript), encoding='unicode')
            xml_selector = Selector(text=xml)
            latitude = xml_selector.xpath('.//var[@name="prop_lat"]/number/@value').extract_first()
            longitude = xml_selector.xpath('.//var[@name="prop_lng"]/number/@value').extract_first()
            item_loader.add_value('latitude', latitude)
            item_loader.add_value('longitude', longitude)

        room_count = response.xpath('//span[contains(text(),"Bedroom")]/following-sibling::span/text()').extract_first()
        if room_count and room_count.isnumeric():
            item_loader.add_value('room_count', room_count)
        
        bathroom_count = response.xpath('//span[contains(text(),"Bathroom")]/following-sibling::span/text()').extract_first()
        if bathroom_count and bathroom_count.isnumeric():
            item_loader.add_value('bathroom_count', bathroom_count)

        features = response.xpath('//div[@id="features"]//text()').extract()
        if features:
            featuresString = " ".join(features)

            if "elevator" in featuresString.lower() or 'lift' in featuresString.lower(): 
                item_loader.add_value('elevator', True)

            if "balcony" in featuresString.lower():
                item_loader.add_value('balcony', True)

            if "terrace" in featuresString.lower():
                item_loader.add_value('terrace', True)

            if "swimming pool" in featuresString.lower():
                item_loader.add_value('swimming_pool', True)

            if "washing machine" in featuresString.lower():
                item_loader.add_value('washing_machine', True)

            if "dishwasher" in featuresString.lower() or "washer" in featuresString.lower():
                item_loader.add_value('dishwasher', True)
    
            # http://www.veniceproperties.co/property/111/Lambourn-House-London-NW8-Apartment            
            if " furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', True)

            elif "unfurnished" in featuresString.lower() or "un-furnished" in featuresString.lower(): 
                item_loader.add_value('furnished', False)

            if "pets considered" in featuresString.lower(): 
                item_loader.add_value('pets_allowed', True)

        # http://www.veniceproperties.co/property/111/Lambourn-House-London-NW8-Apartment
        parking = response.xpath('//span[contains(text(),"Parking")]/following-sibling::span/text()').extract_first()
        if parking:
            if parking == "Yes":
                item_loader.add_value('parking', True)
            elif parking == "No":
                item_loader.add_value('parking', False)

        area_string = response.xpath('//span[contains(text(),"Area Size")]/following-sibling::span/text()').extract_first()
        area_string = area_string.split()[0]
        if float(area_string) != 0:
            square_meters = round(float(area_string)*0.3048, 2)
            square_meters = str(square_meters)+' sq m'
            item_loader.add_value('square_meters', square_meters)

        rent_string = response.xpath('//a[contains(text(),"£")]/text()').extract_first()
        if rent_string:
            item_loader.add_value('rent_string', rent_string)

        available_date_string = response.xpath('//span[contains(text(),"Available Date")]/following-sibling::span/text()').extract_first()
        if available_date_string:
            available_date_string = available_date_string.strip()
            available_date = format_date(available_date_string, '%d %b %Y')
            item_loader.add_value('available_date', available_date)

        item_loader.add_xpath('images', '//div[@class="fotorama"]//a/@href')
        floorPlanIframeUrl = response.xpath('//div[@id="floorplan"]//iframe/@src').extract_first()
        if floorPlanIframeUrl:
            floorPlanIframeUrl = "http://www.veniceproperties.co/"+floorPlanIframeUrl.lstrip(' /')
            item_loader.add_value('floor_plan_images', Selector(text=requests.get(floorPlanIframeUrl).text).xpath('.//img/@src').extract_first())

        item_loader.add_value('landlord_name', "Venice Properties Ltd")
        item_loader.add_value('landlord_phone', '02077247862')
        item_loader.add_value('landlord_email', 'info@veniceproperties.co')
        item_loader.add_value("external_source", "Veniceproperties_PySpider_{}_{}".format(self.country, self.locale))
        item_loader.add_value("external_link", response.meta.get("request_url"))

        self.position += 1
        item_loader.add_value("position", self.position)
        yield item_loader.load_item()

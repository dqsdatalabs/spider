# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

import scrapy
import re
from ..loaders import ListingLoader
from python_spiders.helper import string_found
import dateparser

def extract_city_zipcode(_address):
    zip_city = _address.split(", ")[1]
    zipcode, city = zip_city.split(" ")
    return zipcode, city

class LantmeetersSpider(scrapy.Spider):
    name = "lantmeeters"
    allowed_domains = ["www.lantmeeters.be"]
    start_urls = (
        'http://www.www.lantmeeters.be/',
    )
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    thousand_separator='.'
    scale_separator=','

    def start_requests(self):
        start_urls = [
            {'url': 'https://www.lantmeeters.be/residentieel/te-huur/appartementen', 'property_type': 'apartment'}
        ]
        for url in start_urls:
            yield scrapy.Request(
                url=url.get('url'),
                callback=self.parse, meta={'property_type': url.get('property_type')},
                dont_filter=True
            )

    def parse(self, response, **kwargs):
        links = response.xpath('//section[@id="properties__list"]/ul/li')
        for link in links:
            url = link.xpath('./a[contains(@class, "property-properties")][contains(@id, "property")]/@href').extract_first('')
            if 'referenties' in url or not url:
                continue
            yield scrapy.Request(url=url, callback=self.get_property_details, meta={'property_type': response.meta.get('property_type')})        
    
    def get_property_details(self,response):
        landlord_name = response.xpath('//div[@class="name"]/text()').extract_first()
        property_type = response.meta.get('property_type')
        landlord_phone = response.xpath('//div[@class="name"]/following-sibling::a/text()').extract_first()
        address = response.xpath('//div[@class="location"]/text()').extract_first().strip()
        zipcode, city = extract_city_zipcode(address)
        floor_plan_images = []
        floor_plan_imgs = response.xpath('//a[contains(text(), "Plan")]|//a[contains(text(), "plan")]')
        for floor_plan_img in floor_plan_imgs:
            floor_plan_img_v = floor_plan_img.xpath('./@href').extract_first() 
            floor_plan_images.append(floor_plan_img_v)
        external_id = response.url.split('/')[-1]
        pets_allowed_text = response.xpath('//dt[contains(text(), "Huisdieren toegelaten")]/following-sibling::dd/text()').extract_first()
        if pets_allowed_text and 'ja' in pets_allowed_text.lower():
            pets_allowed = True
        else:
            pets_allowed = ''
        elevator_text = response.xpath('//dt[contains(text(), "Lift")]/following-sibling::dd/text()').extract_first()
        if elevator_text and 'ja' in elevator_text.lower():  
            elevator = True
        else:
            elevator = ''
        parking_xpath = response.xpath('//dt[contains(text(), "Parking") or contains(text(), "Garages ")]/following-sibling::dd/text()').extract_first('')
        if parking_xpath and int(parking_xpath):
            parking = True
        else:
            parking = ''
        item_loader = ListingLoader(response=response)
        if property_type:
            item_loader.add_value('property_type', property_type)
        item_loader.add_value("external_link", response.url)
        item_loader.add_value('external_id', external_id)
        item_loader.add_value('address', address)
        item_loader.add_value('city', city)
        item_loader.add_value('zipcode', zipcode)
        if pets_allowed:
            item_loader.add_value('pets_allowed', True)
        if elevator:
            item_loader.add_value('elevator', True)
        if parking:
            item_loader.add_value('parking', True)
        bathroom_count = response.xpath('//dt[contains(text(), "Badkamers")]/following-sibling::dd/text()').extract_first()
        if bathroom_count:
            item_loader.add_value('bathroom_count', bathroom_count)
        available_date = response.xpath('//dt[contains(text(), "Beschikbaarheid")]/following-sibling::dd/text()').extract_first()
        if available_date:
            date_parsed = dateparser.parse(available_date.replace("Onmiddellijk","now").split(" - ")[-1], date_formats=["%d/%m/%Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))
        item_loader.add_xpath('floor', '//dt[contains(text(), "Aantal verdiepingen")]/following-sibling::dd/text()')
        item_loader.add_value('floor_plan_images', floor_plan_images)
        item_loader.add_xpath('title', '//title/text()')
        item_loader.add_xpath('description', '//section[@id="property-description"]/div/text()')
        item_loader.add_xpath('rent_string', '//div[@class="price"]/text()')
        item_loader.add_xpath('utilities', '//div[dt[.="Prijs kosten"]]/dd/text()')
        item_loader.add_xpath('images', '//section[@id="property-photos"]/ul/li//img/@src')
        item_loader.add_xpath('square_meters', '//dt[contains(text(), "Bewoonbare opp")]/following-sibling::dd/text()')
        item_loader.add_xpath('room_count', '//dt[contains(text(), "Slaapkamers")]/following-sibling::dd/text()')
        latlng=response.xpath("//script[contains(.,'lng')]/text()").get()
        if latlng:
            latitude=latlng.split("lat")[-1].split(";")[0].replace("=","").strip()
            longitude=latlng.split("lng")[-1].split(";")[0].replace("=","").strip()
            item_loader.add_value("longitude",longitude)
            item_loader.add_value("latitude",latitude)
        item_loader.add_value('landlord_name', landlord_name)
        item_loader.add_value('landlord_email', 'info@lantmeeters.be')
        item_loader.add_value('landlord_phone', landlord_phone)
        item_loader.add_value("external_source", "{}_PySpider_{}_{}".format(self.name.capitalize(), self.country, self.locale))
        yield item_loader.load_item()

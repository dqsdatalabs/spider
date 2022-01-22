# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import re
import dateparser

class MySpider(Spider):
    name = 'immohaven_be'
    execution_type = 'testing'
    country = 'belgium'
    locale ='nl'
    external_source = 'Immohaven_PySpider_belgium'
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.immohaven.be/te-huur/appartementen",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.immohaven.be/te-huur/woningen",
                ],
                "property_type": "house"
            },
           
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    url=item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//div[@class='grid grid-space__properties']/div/a/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        property_type = response.meta.get('property_type')
        item_loader.add_value("property_type", property_type)
        
        external_id = response.url
        if external_id:
            item_loader.add_value("external_id", external_id.split("/")[-1])

        title = response.xpath("//p[@class='detail_meta__title']/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        rent = response.xpath("//p[@class='detail_meta__price']/text()").get()
        if rent:
            rent = rent.split("€")[1].split("p/m")[0].strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        address = " ".join(response.xpath("//p[@class='detail_meta__address']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(" ")[-1]
            if city:
                item_loader.add_value("city", city.strip())  
        
        room_count = response.xpath("//span[contains(.,'Slaapkamer')]/preceding-sibling::span/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)
        else:
            room_count = response.xpath("//li[@data-feature='rooms']/span[1]/text()").get()
            if room_count:
                item_loader.add_value("room_count", room_count)
        
        square_meters = response.xpath("//span[contains(.,'Bewoon')]/preceding-sibling::span/text()").get()
        if square_meters:
           item_loader.add_value("square_meters", square_meters.split(",")[0].split(" ")[0].strip())
        
        utilities = response.xpath("//p[contains(.,'lasten huurder')]/following-sibling::p/text()").get()
        if utilities:
            utilities = utilities.split("€")[1].split("p/m")[0].strip()
            item_loader.add_value("utilities", utilities)
        
        floor = response.xpath("//p[contains(.,'verdieping')]/following-sibling::p/text()").get()
        if floor:
            if int(floor) > 0:
                item_loader.add_value("floor", floor)
        
        elevator = response.xpath("//p/text()[contains(.,'lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        parking = response.xpath("//p/text()[contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//p/text()[contains(.,'Autostaanplaats')]").get()
            if parking:
                item_loader.add_value("parking", True)
        
        energy_label = response.xpath("//p[contains(.,'EPC')]/following-sibling::p/span/@class").get()
        if energy_label:
            energy_label = energy_label.split("epc--")[1].split(" ")[0].strip()
            item_loader.add_value("energy_label", energy_label.upper())

        available_date = response.xpath("//p[contains(.,'beschikbaarheid')]/following-sibling::p/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        desc = " ".join(response.xpath("//div[@class='detail_description']/text()").getall())
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description.strip())
            
        images = [x for x in response.xpath("//a[@class='lg_photo']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude_longitude = "".join(response.xpath("//script/text()[contains(.,'lat')]").getall())
        if latitude_longitude:
            latitude = latitude_longitude.split('lat = ')[1].split(';')[0].strip()
            longitude = latitude_longitude.split('lng = ')[1].split(';')[0].strip()
            item_loader.add_value("latitude", latitude)
            item_loader.add_value("longitude", longitude)
        
        item_loader.add_value("landlord_name", "Immo Haven")
        item_loader.add_value("landlord_phone", "011 74 30 60")
        item_loader.add_value("landlord_email", "info@immohaven.be")
        
        yield item_loader.load_item()
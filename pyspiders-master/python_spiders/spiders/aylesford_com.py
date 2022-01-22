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

class MySpider(Spider):  
    name = 'aylesford_com'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.aylesford.com/properties-to-rent/london/apartment",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.aylesford.com/properties-to-rent/london/house",
                ],
                "property_type" : "house"
            },   
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//h3[@class='item-address']"):
            follow_url = response.urljoin(item.xpath("./a/@href").get())
            room_count = item.xpath("./..//li[@class='bedrooms']/text()").get()
            bathroom_count = item.xpath("./..//li[@class='bathrooms']/text()").get()
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type'), 'room_count': room_count, 'bathroom_count': bathroom_count})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        if response.url == 'https://www.aylesford.com/properties-to-rent/london': return

        item_loader.add_value("external_source", "Aylesford_PySpider_"+ self.country + "_" + self.locale)

        external_id = response.xpath("//li[contains(text(),'ID:')]/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(':')[-1].strip())
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            city = address.split(',')[-2].strip()
            zipcode = address.split(',')[-1].strip()
            item_loader.add_value("city", city.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip())

        description = " ".join(response.xpath("//div[@class='intro-text']/p//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//ul[@class='list-details']//li[contains(text(),'Sq ')]/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.lower().split('sq m')[0].split('/')[-1].strip())
        
        room_count = response.meta.get('room_count')
        if room_count:
            item_loader.add_value("room_count", room_count.split('-')[0].strip())
 
        bathroom_count = response.meta.get('bathroom_count')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split('-')[0].strip())
        
        rent = response.xpath("//span[@class='per_month_price_content  ']//span[@class='price-qualifier']/@data-price").get()
        if rent:
            # term = response.xpath("//strong[@class='meta-price']/text()").get()
            # if term and 'per week' in term.lower():
            rent=rent.replace(",","")
            item_loader.add_value("rent", rent)
            item_loader.add_value("currency", 'GBP')

        images = [x for x in response.xpath("//div[@class='slider-images']//li[@class='slide']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [x for x in response.xpath("//div[@id='floorplan-modal']//div[@class='floorplan-image']//img/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'setLocratingIFrameProperties')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split("'lat':")[-1].split(',')[0].strip())
        
        longitude = response.xpath("//script[contains(.,'setLocratingIFrameProperties')]/text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude.split("'lng':")[-1].split(',')[0].strip())

        energy_label = response.xpath("//li[contains(.,'Energy Efficiency Rating')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split('-')[-1].strip())

        parking = response.xpath("//p[contains(text(),'Accomodation')]/text()[contains(.,'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//li[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        elevator = response.xpath("//li[contains(.,'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        item_loader.add_value("landlord_name", 'Aylesford International')
        item_loader.add_value("landlord_phone", '+44 (0)20 7351 2383')

        yield item_loader.load_item()

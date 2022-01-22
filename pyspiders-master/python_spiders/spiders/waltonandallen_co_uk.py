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
from word2number import w2n

class MySpider(Spider):
    name = 'waltonandallen_co_uk'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.waltonandallen.co.uk/property-results/?location=&radius=30&search-type=to-let&property_type=apartment&low_price=&low_rent=&high_price=&high_rent=&bedrooms=0&bathrooms=0&reception_rooms=0&lat=52.9667&long=-1.1667",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.waltonandallen.co.uk/property-results/?location=&radius=30&search-type=to-let&property_type=mid-terraced-house&low_price=&low_rent=&high_price=&high_rent=&bedrooms=0&bathrooms=0&reception_rooms=0&lat=52.9667&long=-1.1667",
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

        for item in response.xpath("//p[@class='h4']/../@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[@class='nextpostslink']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Waltonandallen_PySpider_"+ self.country + "_" + self.locale)

        address = response.xpath("//address/h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("title", address.strip())
            if "Nottingham" in address:
                item_loader.add_value("city", "Nottingham")
            else:
                city = address.strip().rstrip(",").split(",")[-1]
                item_loader.add_value("city", city)

        description = " ".join(response.xpath("//div[@id='panel1']//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        room_count = response.xpath("//li[contains(.,'bedroom')]/text()").get()
        if room_count:
            room_count = room_count.lower().split('bedroom')[0].strip()
            try:
                room_count = w2n.word_to_num(room_count)
                if room_count != 0:
                    item_loader.add_value("room_count", str(room_count))
            except:
                pass
        
        bathroom_count = response.xpath("//li[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.lower().split('bathroom')[0].strip()
            if bathroom_count != '':
                item_loader.add_value("bathroom_count", bathroom_count)
            else:
                item_loader.add_value("bathroom_count", '1')
        
        rent = response.xpath("//p[@class='property-price']/text()").get()
        if rent:
            item_loader.add_value("rent", rent.split('£')[-1].replace(',', '').strip())
            item_loader.add_value("currency", 'GBP')
        
        images = [x for x in response.xpath("//div[@class='slider-for hide']//img/@src").getall()]
        for image in response.xpath("//div[@class='slider-for hide']//img/@data-lazy").getall():
            images.append(image)
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [x for x in response.xpath("//div[@id='panel3']/img/@data-src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        latitude = response.xpath("//div[@id='mapTab']/div/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude.strip())
        
        longitude = response.xpath("//div[@id='mapTab']/div/@data-lng").get()
        if longitude:
            item_loader.add_value("longitude", longitude.strip())
        
        parking = response.xpath("concat(//li[contains(.,'parking')]/text(), ' ', //li[contains(.,'Parking')]/text())").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//li[contains(.,'Balcony') or  contains(.,'balcony') ]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        furnished = response.xpath("//li[contains(.,'Fully furnished') or contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//li[contains(.,'Unfurnished')]").get()
            if furnished:
                item_loader.add_value("furnished", False)

        elevator = response.xpath("//li[contains(.,'Lift access')]").get()
        if elevator:
            item_loader.add_value("elevator", True)
        
        swimming_pool = response.xpath("//li[contains(.,'swimming pool')]").get()
        if swimming_pool:
            item_loader.add_value("swimming_pool", True)

        balcony = response.xpath("//li[contains(.,'balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        item_loader.add_value("landlord_phone", '0115 924 3304')
        item_loader.add_value("landlord_email", 'info@waltonandallen.co.uk')
        item_loader.add_value("landlord_name", 'Walton & Allen - Estate Agents')

        yield item_loader.load_item()

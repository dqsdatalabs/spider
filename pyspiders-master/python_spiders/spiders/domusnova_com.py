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
    name = 'domusnova_com' 
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "http://domusnova.com/properties/rent/?proptype=3&pricefrom=&priceto=&minbed=&propertyarea=0&furnished=&lettingperiod=2&glarea=&location=&keywords=&sortby=pricehigh",
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "http://domusnova.com/properties/rent/?proptype=2&pricefrom=&priceto=&minbed=&propertyarea=0&furnished=&lettingperiod=2&glarea=&location=&keywords=&sortby=pricehigh",
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

        for item in response.xpath("//a[contains(@class,'image-overlay hyperlink')]/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        p_type = response.xpath("//span[@class='lgwhite']/text()").get()
        if p_type and "studio" in p_type.lower():
            p_type = "studio"
        else:
            p_type = response.meta.get('property_type')
        item_loader.add_value("property_type", p_type)
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Domusnova_PySpider_united_kingdom")

        external_id = response.url.split('rent/')[1].split('/')[0]
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        address = response.xpath("//div[@class='cLeft property-title']//h2/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("title", address.strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
        
        description = " ".join(response.xpath("//div[@id='property-details-accordion']//p//text()").getall()).strip()   
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))

        square_meters = response.xpath("//span[contains(text(),'sq m')]/text()").get()
        if square_meters:
            square_meters = square_meters.lower().split('sq m')[0].strip().split(" ")[-1]
            if square_meters:
                item_loader.add_value("square_meters", int(float(square_meters)))

        room_count = response.xpath("//span[@class='lgwhite']/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.lower().split('bedroom')[0].split('(')[-1].strip())

        if not item_loader.get_collected_values("room_count"):
            rc = response.xpath("//li[contains(.,'bedroom')]/span/text()[contains(.,'One') or contains(.,'Two') or contains(.,'Three') or contains(.,'Four')]").get()
            if rc:
                if 'one' in rc.lower(): item_loader.add_value("room_count", 1)
                if 'two' in rc.lower(): item_loader.add_value("room_count", 2)
                if 'three' in rc.lower(): item_loader.add_value("room_count", 3)
                if 'four' in rc.lower(): item_loader.add_value("room_count", 4)
        
        bathroom_count = response.xpath("//span[contains(text(),'bathroom')]/text()").get()
        if bathroom_count:
            try:
                item_loader.add_value("bathroom_count", w2n.word_to_num(bathroom_count.strip().split(' ')[0].strip()))
            except:
                pass

        rent = response.xpath("//div[@class='cLeft property-price']/text()").get()
        if rent:
            rent = rent.split('£')[-1].split('p/w')[0].strip().replace(',', '').replace('\xa0', '')
            item_loader.add_value("rent", str(int(float(rent)) * 4))
            item_loader.add_value("currency", 'GBP')
        
        images = [response.urljoin(x) for x in response.xpath("//ul[@id='mycarousel']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//img[contains(@src,'floorplan')]/@src").getall()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude = response.xpath("//script[contains(.,'\"Lat\"')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('var _property =')[1].split(';')[0].split('"Lat":')[1].split(',')[0].strip())
            item_loader.add_value("longitude", latitude.split('var _property =')[1].split(';')[0].split('"Lon":')[1].split(',')[0].strip())
        
        energy_label = response.xpath("//span[contains(text(),'EPC=')]/text()").get()
        if energy_label:
            item_loader.add_value("energy_label", energy_label.split('EPC=')[-1].strip())
        
        parking = response.xpath("//span[contains(text(),'parking') or contains(text(),'Parking')]").get()
        if parking:
            item_loader.add_value("parking", True)

        balcony = response.xpath("//span[contains(text(),'balcony') or contains(text(),'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        elevator = response.xpath("//span[contains(text(),'lift') or contains(text(),'Lift')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        terrace = response.xpath("//span[contains(text(),'terrace') or contains(text(),'Terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)

        item_loader.add_value("landlord_name", "Domus Nova")
        item_loader.add_value("landlord_email", "lettings@domusnova.com")

        yield item_loader.load_item()

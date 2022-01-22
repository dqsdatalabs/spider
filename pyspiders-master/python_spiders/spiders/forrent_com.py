# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'forrent_com'
    execution_type='testing' 
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.forrent.com/find/CT/metro-Central+CT/Bristol/extras-Apartment?bounds=41.742013931770664,41.65947410885726,-72.85754167080077,-72.98793565902079",
                ],
                "property_type": "apartment"
            },
	        {
                "url": [
                    "https://www.forrent.com/find/CT/metro-Central+CT/Bristol/extras-House?bounds=41.742013931770664,41.65947410885726,-72.85754167080077,-72.98793565902079",
                    "https://www.forrent.com/find/CT/metro-Central+CT/Bristol/extras-Townhouse?bounds=41.742013931770664,41.65947410885726,-72.85754167080077,-72.98793565902079"
                ],
                "property_type": "house"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            for item in url.get('url'):
                yield Request(
                    item,
                    callback=self.parse,
                    meta={'property_type': url.get('property_type')}
                )

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[@class='gallery-container']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

        next_page = response.xpath("//a[@aria-label='next page']/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={"property_type": response.meta.get('property_type')})
        
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Forrent_PySpider_united_kingdom")

        title = response.xpath("//h1/text()").get()
        item_loader.add_value("title", title)
        externalid=response.url
        if externalid: 
            item_loader.add_value("external_id",externalid.split("/")[-1])
        
        address = response.xpath("//p[@data-qaid='address']/text()").get()
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
            
        rent = response.xpath("//td[@class='table-data']/text()[contains(.,'$')]").get()
        if rent:
            item_loader.add_value("rent", rent.replace(",","").replace("$",""))
        item_loader.add_value("currency", "AUD")
        
        room_count = response.xpath("//td[contains(@class,'table-data')]/text()[contains(.,'Bed')]").get()
        if room_count:
            if "-" in room_count:
                item_loader.add_value("room_count",room_count.split("-")[-1].split("B")[0])
            else:
                item_loader.add_value("room_count", room_count.split(" ")[0])

        
        bathroom_count = response.xpath("//td[contains(@class,'table-data')]/text()[contains(.,'Bath')]").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0].split(".")[0])
        
        square_meters = response.xpath("//td[contains(@class,'table-data')]/text()[contains(.,'sq')]").get()
        if square_meters:
            square_meters = square_meters.split(" ")[0].replace(",","")
            item_loader.add_value("square_meters", str(int(int(square_meters)* 0.09290304)))
        
        
        item_loader.add_xpath("latitude", "//meta[@property='place:location:latitude']/@content")
        item_loader.add_xpath("longitude", "//meta[@property='place:location:longitude']/@content")
        
        description = " ".join(response.xpath("//section[@class='property-details']//p//text()").getall())
        if description:
            item_loader.add_value("description", description.strip())
        
        washing_machine = response.xpath("//li[contains(.,'Washer')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        balcony = response.xpath("//li[contains(.,'Balcon')]").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        parking = response.xpath("//li[contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        dishwasher = response.xpath("//li[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        if response.xpath("//li[contains(@class,'slide')]//@src").getall():
            images = [x for x in response.xpath("//li[contains(@class,'slide')]//@src").getall()]
            item_loader.add_value("images", images)
        else:
            images = response.xpath("//script[contains(.,'image\": [')]/text()").get()
            if images:
                item_loader.add_value("images", images.split('"image": [')[1].split(']')[0].replace('"','').strip())
        
        item_loader.add_value("landlord_name", "ForRent.com")
        item_loader.add_value("landlord_phone", "(888) 658-7368")
        item_loader.add_value("landlord_email", "support@apartments.com")
        
        status = response.xpath("//span[@class='no-availability-label']/text()").get()
        if not status: 
            yield item_loader.load_item()
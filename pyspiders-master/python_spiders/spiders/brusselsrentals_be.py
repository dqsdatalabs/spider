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
    name = 'brusselsrentals_be'
    execution_type = 'testing'
    country = 'belgium'
    locale = 'en'
    external_source = 'Brusselsrentals_PySpider_belgium'
    
    def start_requests(self):
        start_urls = [
            {
                "url": [
                    "https://www.brusselsrentals.be/apartment-to-rent",
                ],
                "property_type": "apartment"
            },
            {
                "url": [
                    "https://www.brusselsrentals.be/house-to-rent",
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
        
        for item in response.xpath("//a[contains(.,'DETAIL')]/@href").getall():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={"property_type": response.meta.get('property_type')})

        next_page = response.xpath("(//a[@rel='next']/@href)[1]").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_source", self.external_source)
        item_loader.add_value("external_link", response.url)
        property_type = response.meta.get('property_type')
        item_loader.add_value("property_type", property_type)
        
        external_id = response.xpath("//div[@class='ref']/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.split(": ")[1])

        title = response.xpath("//div[@class='details-header2']/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        rent = response.xpath("//div[@class='details-header2']/h4/text()").get()
        if rent:
            rent = rent.split("€")[1].split("/")[0].replace(",","").strip()
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "EUR")
        
        address = " ".join(response.xpath("//span[@class='road']/text()").getall())
        if address:
            item_loader.add_value("address", address.strip())
            
        item_loader.add_value("city", "Brussels")  
        
        room_count = response.xpath("//div[@class='details-header2']//img[@alt='Beds']/following-sibling::text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[@class='details-header2']//img[@alt='Baths']/following-sibling::text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())

        square_meters = response.xpath("//div[@class='details-header2']//img[@alt='Internal Area']/following-sibling::text()").get()
        if square_meters:
           item_loader.add_value("square_meters", square_meters.replace("m","").strip())
        
        elevator = response.xpath("//div[@class='item-ai']/text()[contains(.,'Elevator')]").get()
        if elevator:
            item_loader.add_value("elevator", True)

        furnished = response.xpath("//div[@class='item-ai']/text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        dishwasher = response.xpath("//div[@class='item-ai']/text()[contains(.,'Dishwasher')]").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        
        balcony = response.xpath("//div[@class='item-ai']/text()[contains(.,'Balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        parking = response.xpath("//div[@class='item-ai']/text()[contains(.,'Garage')]").get()
        if parking:
            item_loader.add_value("parking", True)
        else:
            parking = response.xpath("//div[@class='item-ai']/text()[contains(.,'Parking')]").get()
            if parking:
                item_loader.add_value("parking", True)

        available_date = response.xpath("//strong[contains(.,'Date Available')]/following-sibling::text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date, date_formats=["%d %m %Y"])
            if date_parsed:
                item_loader.add_value("available_date", date_parsed.strftime("%Y-%m-%d"))

        desc = " ".join(response.xpath("//div[@id='tabs-1']//text()").getall())
        if desc:
            description = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", description.strip())
            
        images = [x for x in response.xpath("//div[contains(@class,'image-box')]/a/@href").getall()]
        if images:
            item_loader.add_value("images", images)
        
        latitude = response.xpath("//div[@id='map']/@data-lat").get()
        if latitude:
            item_loader.add_value("latitude", latitude)
        longitude = response.xpath("//div[@id='map']/@data-lon").get()
        if longitude:
            item_loader.add_value("longitude", longitude)

        item_loader.add_value("landlord_name", "Brussels Rentals")
        item_loader.add_value("landlord_phone", "+32(0)22303815")
        item_loader.add_value("landlord_email", "sherwin@brusselsrentals.be")
        
        yield item_loader.load_item()
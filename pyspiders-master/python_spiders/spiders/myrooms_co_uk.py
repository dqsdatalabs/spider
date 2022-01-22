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
    name = 'myrooms_co_uk_disabled'    
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://myrooms.co.uk/properties/?locations=&zip=&property_name=&street_name=&tube=&area=&zip_free=",
                ],
                "property_type" : "room"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//div[contains(@class,'room-body')]//@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
        
        next_page = response.xpath("//a[contains(.,'Next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse, meta={'property_type': response.meta.get('property_type')})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Myrooms_PySpider_"+ self.country + "_" + self.locale)

        external_id = response.xpath("//div[contains(text(),'Property ID')]/following-sibling::div/text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id.strip())

        title = response.xpath("//div[@class='room-content']/h1/text()").get()
        if title:
            item_loader.add_value("title", title.strip())
        
        description = " ".join(response.xpath("//section[@class='info-box cols'][3]//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description)

        square_meters = response.xpath("//div[contains(text(),'Size')]/following-sibling::div/text()").get()
        if square_meters:
            item_loader.add_value("square_meters", square_meters.lower().split('sq')[0].strip().strip('~'))
        
        room_count = response.xpath("//div[contains(text(),'Bedroom')]/following-sibling::div/text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//div[contains(text(),'Bathroom')]/following-sibling::div/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.strip())
        
        rent = response.xpath("//div[contains(text(),'Monthly Rent')]/following-sibling::div/text()").get()
        if rent:
            rent = rent.split('£')[-1].strip().replace(',', '')
            item_loader.add_value("rent", str(int(float(rent))))
            item_loader.add_value("currency", 'GBP')
        else:
            rent = "".join(response.xpath("//div[@class='room-price']/text()").getall())
            if rent:
                rent = rent.split('£')[-1].strip().replace(',', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))
                item_loader.add_value("currency", 'GBP')
        
        available_date = response.xpath("//span[contains(text(),'Available from')]/text()").get()
        if available_date:
            available_date = available_date.split('from')[-1].strip().split(' ')[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d %B %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        deposit = response.xpath("//div[contains(text(),'Deposit')]/following-sibling::div/text()").get()
        if deposit:
            item_loader.add_value("deposit", str(int(float(deposit.split('£')[-1].strip()))))

        images = [x for x in response.xpath("//a[@data-fancybox='gallery']/@href").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        latitude = response.xpath("//script[contains(.,'map_addresses')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('"lat":')[-1].split(',')[0].strip())
        
        longitude = response.xpath("//script[contains(.,'map_addresses')]/text()").get()
        if longitude:
            item_loader.add_value("longitude", longitude.split('"lng":')[-1].split(',')[0].strip())
        
        address = response.xpath("//script[contains(.,'map_addresses')]/text()").get()
        if address:
            address = address.split(',"address":"')[-1].split('"')[0].strip()
            city = address.split(",")[-2].strip().split(" ")[0]
            zipcode = address.split(",")[-2].replace("London","").strip()
            item_loader.add_value("address", address)
            item_loader.add_value("zipcode", zipcode)
            item_loader.add_value("city", city)

        furnished = response.xpath("//h2[contains(@class,'property-subtitle')]//text()[contains(.,'Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        item_loader.add_value("landlord_name", "Myrooms")
        item_loader.add_value("landlord_phone", '+44 (0)2074999070')
        item_loader.add_value("landlord_email", 'info@myrooms.co.uk')

        yield item_loader.load_item()

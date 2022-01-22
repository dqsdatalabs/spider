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
    name = 'cherrypickedproperties_co_uk'    
    execution_type='testing'
    country = 'united_kingdom'
    locale = 'en' 

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.cherrypickedproperties.co.uk/?id=41389&do=search&for=2&type%5B%5D=7&minbeds=0&maxprice=99999999999&Search=&id=41389&order=2&page=0&do=search",
                    "https://www.cherrypickedproperties.co.uk/?id=41389&do=search&for=2&type%5B%5D=8&minbeds=0&maxprice=99999999999&Search=&id=41389&order=2&page=0&do=search",
                    
                ],
                "property_type" : "apartment"
            },
            {
                "url" : [
                    "https://www.cherrypickedproperties.co.uk/?id=41389&do=search&for=2&type%5B%5D=6&minbeds=0&maxprice=99999999999&Search=&id=41389&order=2&page=0&do=search",
                    "https://www.cherrypickedproperties.co.uk/?id=41389&do=search&for=2&type%5B%5D=15&minbeds=0&maxprice=99999999999&Search=&id=41389&order=2&page=0&do=search",
                    "https://www.cherrypickedproperties.co.uk/?id=41389&do=search&for=2&type%5B%5D=16&minbeds=0&maxprice=99999999999&Search=&id=41389&order=2&page=0&do=search",
                   
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.cherrypickedproperties.co.uk/?id=41389&do=search&for=2&type%5B%5D=24&minbeds=0&maxprice=99999999999&Search=&id=41389&order=2&page=0&do=search",
                    
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
        
        page = response.meta.get("page", 1)

        seen = False
        for item in response.xpath("//a[@class='btn-primary-inverse']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item, meta={'property_type': response.meta.get('property_type')})
            seen = True
        
        if page == 1 or seen:
            p_url = response.url.split("&page")[0] + f"&page={page}"
            yield Request(p_url, callback=self.parse, meta={'property_type': response.meta.get('property_type'), "page":page+1})
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
    
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        item_loader.add_value("external_source", "Cherrypickedproperties_Co_PySpider_united_kingdom")

        external_id = response.url.split('pid=')[-1].strip()
        if external_id:
            item_loader.add_value("external_id", external_id)
        
        address = response.xpath("//h1/text()").get()
        if address:
            item_loader.add_value("address", address.strip())
            item_loader.add_value("city", address.split(',')[-2].strip())
            item_loader.add_value("zipcode", address.split(',')[-1].strip())
            
        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", title.strip().replace('\xa0', ''))

        description = " ".join(response.xpath(
            "//div[@class='col-xs-12 col-sm-8']/*[self::div[not(contains(.,'Receptions'))] or self::p]//text()").getall()).strip()
        if description:
            item_loader.add_value("description", description.replace('\xa0', ''))
        
        square_meters = response.xpath("//span[contains(.,'sq ft')]/text()").get()
        if square_meters:
            s_meters = str(int(float(square_meters.split('sq')[0].strip()) * 0.09290304)).strip()
            if s_meters !="0":
                item_loader.add_value("square_meters",s_meters )

        room_count = response.xpath("//span[contains(.,'Bedroom')]/text()").get()
        if room_count:
            if room_count.lower().strip() == 'studio':
                item_loader.add_value("room_count", '1')
            else:
                item_loader.add_value("room_count", room_count.strip())
        
        bathroom_count = response.xpath("//span[contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.lower().split('bathroom')[0].strip())
        
        rent = response.xpath("//h1//../h2/text()").get()
        if rent:
            rent = int(float(rent.split('£')[-1].strip().replace(',', '').replace('\xa0', '')))
            if rent != 0:
                item_loader.add_value("rent", str(rent))
                item_loader.add_value("currency", 'GBP')
        
        if "Holding Deposit" in description:
            deposit = description.split("Holding Deposit")[1].split(":")[1].strip().split(" ")[0]
            if rent != 0:
                deposit = int(deposit) * (rent/4)
                item_loader.add_value("deposit", int(float(deposit)))
                
        available_date = response.xpath("//p[contains(.,'Available from')]/text()").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.split('from')[-1].strip(), date_formats=["%d %B %Y"], languages=['en'])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        
        images = [x for x in response.xpath("//div[@u='slides']//img[@u='image']/@src").getall()]
        if images:
            item_loader.add_value("images", images)
            item_loader.add_value("external_images_count", len(images))
        
        floor = response.xpath("//ul[contains(@class,'features-list')]/text()[contains(.,'Floor') or contains(.,'floor')]").get()
        if floor:
            item_loader.add_value("floor", floor.lower().split('floor')[0].split(',')[-1].strip())
        
        parking = response.xpath("//ul[contains(@class,'features-list')]/text()[contains(.,'Parking') or contains(.,'parking')]").get()
        if parking:
            item_loader.add_value("parking", True)
        
        balcony = response.xpath("//ul[contains(@class,'features-list')]/text()[contains(.,'Balcony') or contains(.,'balcony')]").get()
        if balcony:
            item_loader.add_value("balcony", True)

        furnished = response.xpath("//ul[contains(@class,'features-list')]/text()[contains(.,'Fully Furnished')]").get()
        if furnished:
            item_loader.add_value("furnished", True)

        terrace = response.xpath("//ul[contains(@class,'features-list')]/text()[contains(.,'Terrace') or contains(.,'terrace')]").get()
        if terrace:
            item_loader.add_value("terrace", True)
        
        item_loader.add_value("landlord_phone", '0161 437 3307')
        item_loader.add_value("landlord_email", 'dan@cherrypickedproperties.co.uk')
        item_loader.add_value("landlord_name", 'Cherry Picked Properties')

        yield item_loader.load_item()

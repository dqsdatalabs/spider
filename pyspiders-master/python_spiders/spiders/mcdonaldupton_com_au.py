# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json
import dateparser

class MySpider(Spider):
    name = 'mcdonaldupton_com_au'
    execution_type='testing'
    country='australia'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=Apartment&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=Flat&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=Unit&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=Duplex&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=Semi-detached&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=House&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=Terrace&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=Townhouse&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=Villa&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                ],
                "property_type" : "house"
            },
            {
                "url" : [
                    "https://www.mcdonaldupton.com.au/rent?search=&property_type=Studio&min_rent=0&max_rent=&bedrooms=&bathrooms=",
                ],
                "property_type" : "studio",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item,
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        
        for item in response.xpath("//a[contains(@href,'property_id')]/@href").getall():
            yield Request(response.urljoin(item), callback=self.populate_item, meta={"property_type":response.meta["property_type"]})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Mcdonaldupton_Com_PySpider_australia")    
        item_loader.add_value("external_id", response.url.split("property_id=")[1])    
        item_loader.add_xpath("title","//title/text()")
        item_loader.add_xpath("address", "//div[@class='row']//h1/text()")
    
        city = response.xpath("//div[@class='row']//h1/text()").get()
        if city:
            item_loader.add_value("city", city.split(",")[-1].strip())

        item_loader.add_xpath("room_count", "//div[@class='row']//p/img[contains(@src,'bedroom')]/following-sibling::text()[1]")
        item_loader.add_xpath("bathroom_count", "//div[@class='row']//p/img[contains(@src,'bathroom')]/following-sibling::text()[1]")

        rent = response.xpath("//div[@class='row']//h2/text()").get()
        if rent:
            if "pw" in rent:
                rent = rent.split("$")[-1].split('p')[0].strip().replace(',', '')
                item_loader.add_value("rent", int(float(rent)) * 4)
            else:
                rent = rent.split("$")[-1].split('p')[0].strip().replace(',', '')
                item_loader.add_value("rent", int(float(rent)))
        item_loader.add_value("currency", 'USD')
        available_date = response.xpath("//div[@class='col-lg-9']/p//text()[contains(.,'AVAILABLE ') or contains(.,'Available')]").get()
        if available_date:
            date_parsed = dateparser.parse(available_date.upper().split("AVAILABLE")[1].strip(), date_formats=["%d %m %Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        description = " ".join(response.xpath("//div[@class='col-lg-9']/p//text()").getall()) 
        if description:
            item_loader.add_value("description", description.strip())

        images = [x for x in response.xpath("//div[@id='details-carousel']/ul/li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
    
        parking = response.xpath("//div[@class='row']//p/i[contains(@class,'fa-car')]/following-sibling::text()[1]").get()
        if parking:
            if parking.strip() == "0":
                item_loader.add_value("parking", False)
            else:
                item_loader.add_value("parking", True)
        balcony = response.xpath("//ul[@class='features']/li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        dishwasher = response.xpath("//ul[@class='features']/li[contains(.,'Dishwasher')]/text()").get()
        if dishwasher:
            item_loader.add_value("dishwasher", True)
        script_map = response.xpath("//script[contains(.,'L.marker([')]/text()").get()
        if script_map:
            latlng = script_map.split("L.marker([")[1].split("]")[0]
            item_loader.add_value("latitude", latlng.split(",")[0].strip())
            item_loader.add_value("longitude", latlng.split(",")[1].strip())
    
        item_loader.add_xpath("landlord_name", "//div[@class='col-md-12'][1]/div[@class='desc-box agent-desc']/h4/text()")
        item_loader.add_xpath("landlord_phone", "//div[@class='col-md-12'][1]//p[@class='person-number']/text()[normalize-space()]")
        item_loader.add_xpath("landlord_email", "//input[@id='papf_realestatem']/@value")

        yield item_loader.load_item()
# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import re

class MySpider(Spider):
    name = 'foundersoflondon_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'

    def start_requests(self):
        start_urls = [
            {
                "url": "https://www.foundersoflondon.com/property/lettings?search=&category=lettings&type=apartment&area=&price-min=&price-max=&bedrooms=", 
                "property_type": "apartment"
            },
	        {
                "url": "https://www.foundersoflondon.com/property/lettings?search=&category=lettings&type=house&area=&price-min=&price-max=&bedrooms=", 
                "property_type": "house"
            },
	        {
                "url": "https://www.foundersoflondon.com/property/lettings?search=&category=lettings&type=studio&area=&price-min=&price-max=&bedrooms=", 
                "property_type": "studio"
            }
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})

    # 1. FOLLOWING
    def parse(self, response):
        for item in response.xpath("//div[@class='property-container']"):
            url = item.xpath(".//div[@class='prop-cont-bott-right']/a/@href").get()
            status = item.xpath(".//div[@class='propststxt']/text()[.='let-agreed']").get()
            if not status:
                yield Request(response.urljoin(url), callback=self.populate_item, meta={"property_type":response.meta.get('property_type')})
        
        next_page = response.xpath("//ul[@class='pagination']/li/a[.='Next »']/@href").get()
        if next_page:
            yield Request(next_page, callback=self.parse, meta={"property_type": response.meta.get('property_type')})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_source", "Foundersoflondon_PySpider_united_kingdom")
        item_loader.add_value("external_id", response.url.split("property/")[1].split("/")[0])

        title = response.xpath("//h1//text()").get()
        if title:
            item_loader.add_value("title", title)

        address = response.xpath("//h1//text()").get()
        if address:
            zipcode = address.split(",")[-1]
            if address.count(",")==2:
                city = address.split(",")[-2]
                item_loader.add_value("city", city)
            item_loader.add_value("address", address.strip())
            item_loader.add_value("zipcode", zipcode.strip())

        square_meters = "".join(response.xpath("//span[contains(@class,'propdetbrk')][contains(.,'sq')]//text()").getall())
        if square_meters:
            square_meters = square_meters.split("(")[1].split(")")[0].strip().split(" ")[0]
            item_loader.add_value("square_meters", square_meters.strip())

        rent = response.xpath("//div[contains(@class,'property-det-price')]//span//text()").get()
        if rent:
            rent = rent.replace("£","").replace(",","").strip().split(" ")[0]
            item_loader.add_value("rent", rent)
        item_loader.add_value("currency", "GBP")

        desc = " ".join(response.xpath("//article[contains(@class,'description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)

        room_count = response.xpath("//i[contains(@class,'bed')]//parent::span//parent::span//text()").get()
        if room_count:
            room_count = room_count.strip()
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//i[contains(@class,'bath')]//parent::span//parent::span//text()").get()
        if bathroom_count:
            bathroom_count = bathroom_count.strip()
            item_loader.add_value("bathroom_count", bathroom_count)
        
        images = [x for x in response.xpath("//div[contains(@class,'property-details-photos-wrapper')]//@src").getall()]
        if images:
            item_loader.add_value("images", images)
         
        floor_plan_images = response.xpath("//a[contains(@href,'floor')]//@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        from datetime import datetime
        import dateparser
        available_date = "".join(response.xpath("//div[contains(@class,'property-details-right')]//li[contains(.,'Available from')]//text()").getall())
        if available_date:
            available_date = available_date.split("from")[1].strip()
            if not "now" in available_date.lower():
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)

        balcony = response.xpath("//div[contains(@class,'property-details-right')]//li[contains(.,'Balcon') or contains(.,'balcon')]//text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        terrace = response.xpath("//div[contains(@class,'property-details-right')]//li[contains(.,'Terrace')]//text()").get()
        if terrace:
            item_loader.add_value("terrace", True)

        furnished = response.xpath("//i[contains(@class,'lamp')]//parent::span//parent::span//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)

        elevator = response.xpath("//div[contains(@class,'property-details-right')]//li[contains(.,'Lift')]//text()").get()
        if elevator:
            item_loader.add_value("elevator", True)

        floor = response.xpath("//div[contains(@class,'property-details-right')]//li[contains(.,'Floor') or contains(.,'floor')]//text()[not(contains(.,'Floors'))]").get()
        if floor:
            floor = floor.lower().split("floor")[0].strip()
            item_loader.add_value("floor", floor.strip())

        item_loader.add_value("landlord_name", "FOUNDERS OF LONDON")
        item_loader.add_value("landlord_phone", "+44 (0) 207 183 7693")
   
        yield item_loader.load_item()
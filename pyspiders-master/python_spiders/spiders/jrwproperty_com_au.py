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
from datetime import datetime
from python_spiders.helper import ItemClear 
import re

class MySpider(Spider):
    name = 'jrwproperty_com_au'
    execution_type='testing'
    country='australia'
    locale='en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.jrwproperty.com.au/search-results/{}?property_type%5B%5D=Apartment&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.jrwproperty.com.au/search-results/{}?property_type%5B%5D=House&property_type%5B%5D=Townhouse&property_type%5B%5D=Unit&min_price=&max_price=&bedrooms=&bathrooms=&carspaces=",
                ],
                "property_type" : "house",
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'), "base":item})

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        for item in response.xpath("//div[contains(@class,'container')]/a"):
            status = item.xpath(".//div[@class='sticker']/text()").get()
            if status and ("under" in status.lower() or "let" in status.lower()):
                continue
            follow_url = response.urljoin(item.xpath("./@href").get())
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"]})
            seen = True
        
        if page == 2 or seen:
            base = response.meta["base"]
            slug = f"page/{page}/"
            p_url = base.format(slug)
            yield Request(p_url, callback=self.parse, meta={"property_type":response.meta["property_type"], "base":base, "page":page+1})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        dontallow=response.xpath("//div[@class='suburb-price']/text()").get()
        if dontallow and "under application" in dontallow.lower():
            return
        rented = response.xpath("//label[contains(.,'Contract')]//following-sibling::div//text()[.='Auction']").get()
        if rented:
            return
        external_id = response.xpath("//label[contains(.,'Property ID')]//following-sibling::div//text()").get()
        if external_id:
            item_loader.add_value("external_id", external_id)

        item_loader.add_value("external_source", "Jrwproperty_Com_PySpider_australia")

        title = response.xpath("//h5[contains(@class,'sub-title')]//text()").get()
        if title:
            item_loader.add_value("title", title)

        desc = " ".join(response.xpath("//div[contains(@class,'detail-description')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        room_count = response.xpath("//label[contains(.,'Bedroom')]//following-sibling::div//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count)

        bathroom_count = response.xpath("//label[contains(.,'Bathroom')]//following-sibling::div//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        square_meters = response.xpath("//label[contains(.,'Land Size')]//following-sibling::div//text()").get()
        if square_meters:
            square_meters = square_meters.strip().split(" ")[0]
            item_loader.add_value("square_meters", square_meters)
        
        from datetime import datetime
        import dateparser
        available_date = response.xpath("//label[contains(.,'Available')]//following-sibling::div//text()").get()
        if available_date:
            if "now" in available_date.lower():
                item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
            else:
                date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
        
        price = response.xpath("//div[contains(@class,'suburb-price')]//text()").get()
        if price:
            price = price.strip().replace("/"," ").strip()
            if ",000 -" in price:
                price = price.split("$")[1].replace(",000 -"," ").strip()
            elif "-" in price:
                price = price.split(",000")[0].split("$")[1].strip()
            else:
                if "$" in price:
                    price = price.replace(",000","").split("$")[1].strip().split(" ")[0].strip()
            if ","in price:
                price = price.replace(",","")
            if price.isdigit():
                item_loader.add_value("rent", int(float(price))*4)
        item_loader.add_value("currency", "AUD")
        
            
        deposit = response.xpath("//label[contains(.,'Bond')]//following-sibling::div//text()").get()
        if deposit:
            deposit = deposit.split("$")[1].replace(",","").strip()
            item_loader.add_value("deposit", deposit)
        
        address = response.xpath("//div[contains(@class,'suburb-address')]//text()").get()
        if address:
            city = address.split(",")[-1]
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)

        parking = response.xpath("//label[contains(.,'Garage')]//following-sibling::div//text()").get()
        if parking:
            item_loader.add_value("parking", True)

        furnished = response.xpath("//li[contains(.,'Furnished')]//text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        
        images = [x for x in response.xpath("//div[contains(@class,'image-gallery')]//img//@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        floor_plan_images = response.xpath("//a[contains(@class,'floorplan')]//@src").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)

        latitude_longitude = response.xpath("//script[contains(.,'setView')]/text()").get()
        if latitude_longitude:
            latitude = latitude_longitude.split('setView([')[1].split(',')[0]
            longitude = latitude_longitude.split('setView([')[1].split(',')[1].split(']')[0].strip()      
            item_loader.add_value("longitude", longitude)
            item_loader.add_value("latitude", latitude)

        landlord_name = response.xpath("//div[contains(@class,'agent-detail')]//a//p//text()").get()
        if landlord_name:
            item_loader.add_value("landlord_name", landlord_name)

        landlord_email = response.xpath("//div[contains(@class,'agent-detail')]//p[contains(@class,'email')]//a//text()").get()
        if landlord_email:
            item_loader.add_value("landlord_email", landlord_email)
        
        landlord_phone = response.xpath("//div[contains(@class,'agent-detail')]//p[contains(@class,'phone')]//a//text()").get()
        if landlord_phone:
            item_loader.add_value("landlord_phone", landlord_phone)
        
        status = response.xpath("//label[contains(.,'Contract')]/following-sibling::div/text()").get()
        if "sale" not in status.lower():
            yield item_loader.load_item()
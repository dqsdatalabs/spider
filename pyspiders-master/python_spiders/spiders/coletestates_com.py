# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek


from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json, re
import dateparser
from datetime import datetime


class MySpider(Spider):
    name = 'coletestates_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.coletestates.com/search-results/?search_area=&search_property_type=Residential&search_looking=Rent&search_num_beds_min=&search_num_beds_max=&search_price_rent_low=&search_price_rent_high=&search_price_low=&search_price_high=&search_price_buy_low=&search_price_buy_high=", "property_type": "apartment"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//ul[@id='properties_list']/li"):
            follow_url = item.xpath("./a/@href").get()
            if item.xpath(".//h5[contains(.,'Studio')]/text()").get():
                property_type = "studio"
            else:
                property_type = "apartment"

            yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type})

        pagination = response.xpath("//div[@class='pagination_wrapper']/a[contains(.,'Next')]/@href").get()
        if pagination:
            yield Request(pagination, callback=self.parse, meta={"property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = " ".join(response.xpath("//h2//text()").extract())
        item_loader.add_value("title", re.sub('\s{2,}', ' ', title))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source","Coletestates_PySpider_"+ self.country)
        
        address = response.xpath("//h2/text()").get()
        if address:
            zipcode = address.split(",")[-1]
            city = address.split(zipcode)[0].strip().strip(",")
            item_loader.add_value("address", address)
            item_loader.add_value("city", city)
            item_loader.add_value("zipcode", zipcode.strip())
        
        rent = response.xpath("//h2/em/text()").get()
        if rent:
            price = rent.split(" ")[0].split("£")[1]
            item_loader.add_value("rent", str(int(price)*4))
        
        item_loader.add_value("currency", "GBP")
        
        room_count = response.xpath("//ul[@class='property_spec']/li[contains(.,'bedroom')]/text()").get()
        if response.meta.get("property_type")=="studio":
            item_loader.add_value("room_count", "1")
        elif room_count:
            item_loader.add_value("room_count", room_count.split(" ")[0])
        
        bathroom_count = response.xpath("//ul[@class='property_spec']/li[contains(.,'bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split(" ")[0])
        
        available_date = response.xpath("//ul[@class='property_spec']/li[contains(.,'Available')]/text()").get()
        if available_date:
            available_date = available_date.split("Available")[1].replace("from","").replace("of","")
            date = "{} {} ".format(available_date, datetime.now().year)
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                current_date = str(datetime.now())
                if current_date > date2:
                    date = datetime.now().year + 1
                    parsed = date2.replace(str(date_parsed.year), str(date))
                    item_loader.add_value("available_date", parsed)
                else:
                    item_loader.add_value("available_date", date2)
        
        furnished = response.xpath("//ul[@class='property_spec']/li[contains(.,'furnished') or contains(.,'Furnished')]/text()").get()
        if furnished:
            item_loader.add_value("furnished", True)
        else:
            furnished = response.xpath("//p[text()='Furnished' or text()='furnished']").get()
            if furnished:
                item_loader.add_value("furnished", True)
        
        floor_plan_images = response.xpath("//li/a[contains(.,'Floor')]/@href").get()
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)
        
        desc = "".join(response.xpath("//div[contains(@class,'description')]//text()").getall())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        energy_label = response.xpath("//div[contains(@class,'description')]//p//text()[contains(.,'EPC')]").get()
        if energy_label:
            item_loader.add_value("energy_label" , energy_label.split("Band")[1].replace(":","").strip())
        
        balcony = response.xpath(
            "//ul[@class='property_spec']/li[contains(.,'balcony') or contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony", True)
        
        floor = response.xpath("//ul[@class='property_spec']/li[contains(.,'floor')]/text()").get()
        floor2 = response.xpath("//ul[@class='property_spec']/li[contains(.,'Floor')]/text()").get()
        floors = False
        if floor:
            floor = floor.split("floor")[0].strip()
            if " " in floor and "top" not in floor.lower() and "ground" not in floor.lower():
                floors = floor.split(" ")[-1]
            elif "wood" not in floor.lower() and "top" not in floor.lower():
                floors = floor
        elif floor2:
            floor2 = floor2.split("Floor")[0].strip()
            if " " in floor2 and "top" not in floor2.lower() and "ground" not in floor2.lower() :
                floors = floor2.split(" ")[-1]
            elif "wood" not in floor2.lower() and "top" not in floor2.lower():
                floors = floor2
        
        if floors:
            item_loader.add_value("floor", floors)
        
        washing_machine = response.xpath("//div[contains(@class,'description')]//p//text()[contains(.,'washing machine')]").get()
        if washing_machine:
            item_loader.add_value("washing_machine", True)
        
        images = [ x for x in response.xpath("//div[@class='thumbs_wrapper']//li/img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        item_loader.add_value("landlord_name", "Colet Estates")
        
        phone = response.xpath("//h4[contains(.,'call')]//text()").get()
        if phone:
            item_loader.add_value("landlord_phone", phone.split("call")[1].strip())
            
        item_loader.add_value("landlord_email", "info@coletestates.com")
        
        yield item_loader.load_item()
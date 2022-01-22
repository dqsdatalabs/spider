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
    name = 'capitalpropertiescardiff_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    start_urls = ["https://www.capitalpropertiescardiff.co.uk/property-list.aspx?location=&minprice=&maxprice=&minbeds=&order=b%20a&let=0"]

    # 1. FOLLOWING
    def parse(self, response):

        for item in response.xpath("//a[@class='listview']/@href").extract():
            follow_url = response.urljoin(item)
            yield Request(follow_url, callback=self.populate_item)
        
        next_page = response.xpath("//a[contains(@title,'Next')]/@href").get()
        if next_page:
            yield Request(response.urljoin(next_page), callback=self.parse)
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)

        item_loader.add_value("external_link", response.url)

        title = response.xpath("//div[@id='propertydetailscontent']/h1/text()").get()
        if title and "studio" in title.lower():
             item_loader.add_value("property_type", "studio")
        elif title and ("apartment" in title.lower() or "flat" in title.lower() or "maisonette" in title.lower()):
            item_loader.add_value("property_type", "apartment")
        elif title and "house" in title.lower():
             item_loader.add_value("property_type", "house")
        else:
            return
        
        item_loader.add_value("external_source", "Capitalpropertiescardiff_PySpider_"+ self.country + "_" + self.locale)
        item_loader.add_value("title", title)
        address = title.split(",")[1]
        if address:
            item_loader.add_value("address", address)
            item_loader.add_value("city", address.strip())
        
        room = response.xpath("//ul/li[contains(.,'bed')]/text()").get()
        if "Studio" in title:
            item_loader.add_value("room_count", "1")
        elif room:
            item_loader.add_value("room_count", room.split("bed")[0].strip())
        # elif "Bedroom" in title:
        #     room = title.split("Bedroom")[0].strip()
        #     item_loader.add_value("room_count", w2n.word_to_num(room))
        # elif "bed" in title:
        #     room = title.strip().split("bed").split(" ")[-1].replace("(","")
        #     item_loader.add_value("room_count", room)
        
        bathroom_count = response.xpath("//ul/li[contains(.,'bathroom') or contains(.,'Bathroom')]/text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("bathroom")[0].strip())
        
        available_date = response.xpath("//ul/li[contains(.,'Available from')]/text()").get()
        if available_date:
            available_date = available_date.split("Available from")[1]
            if available_date:
                date_parsed = dateparser.parse(
                    available_date, date_formats=["%d/%m/%Y"]
                )
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)

        rent = response.xpath("//ul/li/strong[contains(.,'PCM')]/text()").get()
        if rent:
            price = rent.split("PCM")[0].replace("£","").replace("PP","").strip()
            item_loader.add_value("rent", price)
        
        item_loader.add_value("currency", "GBP")
        
        external_id = response.xpath("//ul/li/strong[contains(.,'Reference')]/text()").get()
        if external_id:
            external_id = external_id.split(":")[1].strip()
            item_loader.add_value("external_id", external_id)
        
        desc = "".join(response.xpath("//div[@id='propertydetailscontent']/p//text()").getall())
        if desc:
            desc = desc.replace("\u00a3", "").replace("\u00a0","")
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//div[@id='photooptions']//li//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        if "Deposit:" in desc:
            deposit = desc.split("Deposit:")[1].split("(")[0].replace("pp","").strip()
            item_loader.add_value("deposit", deposit)
        
        if "floor" in desc:
            floor = desc.split("floor")[0].strip().split(" ")[-1]
            if floor.replace("st","").isdigit():
                item_loader.add_value("floor", floor.replace("st",""))
            else:
                item_loader.add_value("floor", floor)

        if "unfurnished" in desc:
            item_loader.add_value("furnished", False)
        elif "furnished" in desc:
            item_loader.add_value("furnished", True)

        lat_lng = response.xpath("//script[contains(.,'propertymap')]/text()").get()
        if lat_lng:
            lat = lat_lng.split("propertymap('")[1].split("'")[0]
            lng = lat_lng.split("propertymap('")[1].split("', '")[1].split("'")[0]
            item_loader.add_value("latitude", lat)
            item_loader.add_value("longitude", lng)
        balcony = response.xpath("//ul/li[contains(.,'Balcony')]/text()").get()
        if balcony:
            item_loader.add_value("balcony",True)

        item_loader.add_value("landlord_name", "CAPITAL PROPERTIES CARDIFF")
        item_loader.add_value("landlord_phone", "07917 415386")
        item_loader.add_value("landlord_email", "info@capitalpropertiescardiff.co.uk")

        yield item_loader.load_item()

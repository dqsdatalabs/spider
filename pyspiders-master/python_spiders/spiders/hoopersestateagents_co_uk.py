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
from datetime import datetime

class MySpider(Spider):
    name = 'hoopersestateagents_co_uk'
    execution_type = 'testing'
    country = 'united_kingdom'
    locale = 'en'
    def start_requests(self):
        start_urls = [
            {
                "url" : [
                    "https://www.hoopersestateagents.co.uk/search.ljson?channel=lettings&fragment=tag-flats-or-apartments/status-available",
                ],
                "property_type" : "apartment",
            },
            {
                "url" : [
                    "https://www.hoopersestateagents.co.uk/search.ljson?channel=lettings&fragment=tag-house/status-available",
                    "https://www.hoopersestateagents.co.uk/search.ljson?channel=lettings&fragment=tag-maisonette/status-available"
                    
                ],
                "property_type" : "house"
            },
        ]
        for url in start_urls:
            for item in url.get("url"):
                yield Request(item.format(""),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type')})


    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen = False
        data = json.loads(response.body)        
        try:
            for item in data["properties"]:
                print("-------", item)
                follow_url = response.urljoin(item["property_url"])
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"], "item":item})
                seen = True
        except: pass
        if page == 2 or seen:
            f_url = response.url.replace(f"available/page-{page-1}", "available").replace("available", f"available/page-{page}")
            yield Request(
                f_url,
                callback=self.parse,
                meta={"page":page+1, "property_type":response.meta["property_type"]})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("external_source", "Hoopersestateagents_Co_PySpider_united_kingdom")
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)

        title = response.xpath("//title/text()").get()
        if title:
            item_loader.add_value("title", re.sub('\s{2,}', ' ', title.strip()))
        
        data = response.meta.get('item')
        
        address = data["display_address"]
        if address:
            item_loader.add_value("address", address)
            city = address.split(",")[-1].strip()
            if not city.isalpha():
                item_loader.add_value("zipcode", city)
                item_loader.add_value("city", address.split(",")[-2].strip())
            else:
                item_loader.add_value("city", city)
        
        rent = data["price"]
        if rent:
            if "pw" in rent:
                price = rent.split(" ")[0].split("£")[1]
                item_loader.add_value("rent", int(price)*4)
            else:
                item_loader.add_value("rent", rent.split(" ")[0].split("£")[1])
        item_loader.add_value("currency", "GBP")
        
        room_count = data["bedrooms"]
        if room_count and room_count !='0':
            item_loader.add_value("room_count", room_count)
        
        bathroom_count = data["bathrooms"]
        if bathroom_count and bathroom_count !='0':
            item_loader.add_value("bathroom_count", bathroom_count)
        
        desc = " ".join(response.xpath("//div[contains(@class,'page-content')]//text()").getall())
        if desc:
            desc = re.sub('\s{2,}', ' ', desc.strip())
            item_loader.add_value("description", desc)
        
        images = [x for x in response.xpath("//ul[@class='slides']//img/@src").getall()]
        if images:
            item_loader.add_value("images", images)
        
        external_id = data["property_id"]
        if external_id:
            item_loader.add_value("external_id", str(external_id))
        
        match = re.search(r'(\d+/\d+/\d+)', desc)
        if match:
            newformat = dateparser.parse(match.group(1), languages=['en']).strftime("%Y-%m-%d")
            item_loader.add_value("available_date", newformat)
        elif "AVAILABLE mid" in desc:
            available_date = desc.split("AVAILABLE mid")[1].split("Property")[0].strip()
            date_parsed = dateparser.parse(available_date, date_formats=["%d/%m/%Y"])
            if date_parsed:
                date2 = date_parsed.strftime("%Y-%m-%d")
                item_loader.add_value("available_date", date2)
        elif "available now" in desc.lower():
            item_loader.add_value("available_date", datetime.now().strftime("%Y-%m-%d"))
        
        item_loader.add_value("latitude", str(data["lat"]))
        item_loader.add_value("longitude", str(data["lng"]))
        
        item_loader.add_value("landlord_name", "HOOPERS ESTATE AGENTS")
        item_loader.add_value("landlord_phone", "020 8452 1436")
       
        yield item_loader.load_item()
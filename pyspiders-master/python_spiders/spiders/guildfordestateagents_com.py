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
import re

class MySpider(Spider):
    name = 'guildfordestateagents_com'
    execution_type='testing'
    country='united_kingdom'
    locale='en'
    def start_requests(self):
        start_urls = [
            {"url": "https://www.guildfordestateagents.com/search.ljson?channel=lettings&fragment=tag-apartment/up-to-5000/status-available", "property_type": "apartment"},
            {"url": "https://www.guildfordestateagents.com/search.ljson?channel=lettings&fragment=tag-flat/up-to-5000/status-available", "property_type": "apartment"},
	        {"url": "https://www.guildfordestateagents.com/search.ljson?channel=lettings&fragment=tag-bungalow/up-to-5000/status-available", "property_type": "house"},
            {"url": "https://www.guildfordestateagents.com/search.ljson?channel=lettings&fragment=tag-cottage/up-to-5000/status-available", "property_type": "house"},
            {"url": "https://www.guildfordestateagents.com/search.ljson?channel=lettings&fragment=tag-house/up-to-5000/status-available", "property_type": "house"},
            {"url": "https://www.guildfordestateagents.com/search.ljson?channel=lettings&fragment=tag-maisonette/up-to-5000/status-available", "property_type": "house"},
        ]  # LEVEL 1
        
        for url in start_urls:
            yield Request(url=url.get('url'),
                            callback=self.parse,
                            meta={'property_type': url.get('property_type'),
                        })

    # 1. FOLLOWING
    def parse(self, response):
        property_type = response.meta.get("property_type")
        page = response.meta.get('page', 2)
        data = json.loads(response.body)

        if data["properties"]:
            for item in data["properties"]:
                follow_url = response.urljoin(item["property_url"])
                lat = item["lat"]
                lng = item["lng"]
                yield Request(follow_url, callback=self.populate_item, meta={"property_type":property_type, "lat":lat, "lng":lng})

            if data["pagination"]["has_next_page"]:
                base_url = response.meta.get("base_url", response.url)
                url = base_url + f"/page-{page}"
                yield Request(url, callback=self.parse, meta={"page": page+1, "base_url":base_url, "property_type":property_type})

    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        item_loader.add_value("property_type", response.meta.get('property_type'))
        title = response.xpath("//div//h2[@id='secondary-address']//text()").extract_first()
        if title:
            item_loader.add_value("title", title)
            item_loader.add_value("address", title)
            item_loader.add_value("city", title.split(",")[-2].strip())
            item_loader.add_value("zipcode", title.split(",")[-1].strip())

        lat = response.meta.get("lat")
        lng = response.meta.get("lng")
        if lat != 0 and lng != 0 :
            item_loader.add_value("longitude", str(lat))
            item_loader.add_value("latitude",str(lng))

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Guildfordestateagents_PySpider_united_kingdom")
 
        external_id = response.xpath("substring-after(//div[contains(@class,'propDetails')]//li[contains(.,'Ref')]//text(),':')").extract_first()     
        if external_id:   
            item_loader.add_value("external_id", external_id.strip())
        
        room_count = response.xpath("//div[contains(@class,'propDetails')]//li[contains(.,'Bedroom')]//text()").get()
        if room_count:
            item_loader.add_value("room_count", room_count.split("Bedroom")[0])

        bathroom_count=response.xpath("//div[contains(@class,'propDetails')]//li[contains(.,'Bathroom')]//text()").get()
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count.split("Bathroom")[0])

        rent = response.xpath("//h3[@id='propertyPrice']//text()").extract_first()
        if rent:
            item_loader.add_value("rent_string", rent)    

        available_date = response.xpath("//div[@id='propertyDetails']/p[contains(.,'Property available on')]/text()[normalize-space()]").get()
        if available_date:
            try:
                date_parsed = dateparser.parse(available_date, languages=['en'])
                if date_parsed:
                    date2 = date_parsed.strftime("%Y-%m-%d")
                    item_loader.add_value("available_date", date2)
            except:
                pass
       
        desc = " ".join(response.xpath("//div[@id='propertyDetails']//text()[not(contains(.,'To let'))]").extract())
        if desc:
            item_loader.add_value("description",re.sub('\s{2,}', ' ', desc))
            if "parking" in desc.lower():
                item_loader.add_value("parking",True)
            if "Furnished or unfurnished" in desc.lower():
                pass
            if "unfurnished" in desc.lower():
                item_loader.add_value("furnished",False)
            elif "furnished" in desc.lower():
                item_loader.add_value("furnished",True)
        images = [response.urljoin(x) for x in response.xpath("//div[@id='carousel_contents']/a/img/@src").extract()]
        if images:
            item_loader.add_value("images", images)      
        floor_plan_images = [response.urljoin(x) for x in response.xpath("//li/a[contains(.,'View floor plan')]/@href").extract()]
        if floor_plan_images:
            item_loader.add_value("floor_plan_images", floor_plan_images)   

        item_loader.add_value("landlord_phone", "01483 339977")
        item_loader.add_value("landlord_name", "Guildford Estate Agency")
        yield item_loader.load_item()
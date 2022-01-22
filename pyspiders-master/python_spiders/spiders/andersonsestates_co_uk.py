# -*- coding: utf-8 -*-
# Author: Mehmet Kurtipek

from scrapy.loader.processors import MapCompose
from scrapy import Spider
from scrapy import Request,FormRequest
from scrapy.selector import Selector
from w3lib.html import remove_tags
from python_spiders.loaders import ListingLoader
import json

class MySpider(Spider):
    name = 'andersonsestates_co_uk'
    execution_type='testing'
    country='united_kingdom'
    locale='en' 
    thousand_separator = ','
    scale_separator = '.' 
    def start_requests(self):
        start_urls = [
            {
                "property_type" : "apartment",
                "type" : "Flat"
            },
            {
                "property_type" : "house",
                "type" : "House"
            },
        ]
        for item in start_urls:
            formdata = {
                "propsearchtype": "",
                "searchurl": "/",
                "market": "1",
                "ccode": "UK",
                "view": "",
                "pricetype": "3",
                "pricelow": "",
                "pricehigh": "",
                "propbedr": "",
                "propbedt": "",
                "proptype": item["type"],
                "area": "",
                "statustype": "4",
            }
            yield FormRequest(
                "http://www.andersonsestates.co.uk/results",
                callback=self.parse,
                formdata=formdata,
                #dont_filter=True,
                meta={
                    "property_type":item["property_type"],
                    "type":item["type"]
                })

    # 1. FOLLOWING
    def parse(self, response):
        page = response.meta.get("page", 10)
        seen = False
        for item in response.xpath("//div[@class='results-list']"):
            follow_url = response.urljoin(item.xpath("./div//a/@href").get())
            room = item.xpath(".//div[contains(@class,'res-attributes')]//span[@class='bedroom']/text()").extract_first()
            bathroom = item.xpath(".//span[@class='bathroom']/text()").extract_first()
            parking = item.xpath(".//span[@class='parking']/text()").extract_first()
            yield Request(follow_url, callback=self.populate_item, meta={"property_type":response.meta["property_type"],"room":room,"bathroom":bathroom,"parking":parking})
            seen = True
        
        if page == 10 or seen:
            p_type = response.meta["type"]
            p_url = f"http://www.andersonsestates.co.uk/results?searchurl=%2f&market=1&ccode=UK&pricetype=3&proptype={p_type}&statustype=4&offset={page}"
            yield Request(
                p_url,
                callback=self.parse,
                meta={"property_type":response.meta["property_type"], "type":p_type, "page":page+10})
         
    
    # 2. SCRAPING level 2
    def populate_item(self, response):
        item_loader = ListingLoader(response=response)
        rented = response.xpath("//h1/a/span//text()").extract_first()
        if rented:
            if "Let" in rented:
                return  
        item_loader.add_value("property_type", response.meta.get('property_type'))
        item_loader.add_value("external_link", response.url)
        item_loader.add_value("external_source", "Andersonsestates_Co_PySpider_united_kingdom")
        room_count = response.meta.get('room')
        if room_count and room_count !='0':
            item_loader.add_value("room_count", room_count)
        else:
            elevator = response.xpath("//div[@class='bullets-li']/p[contains(.,'Lift')]//text()").extract_first()
            if elevator:
                item_loader.add_value("elevator",True)

        bathroom_count = response.meta.get('bathroom')
        if bathroom_count:
            item_loader.add_value("bathroom_count", bathroom_count)

        parking = response.meta.get('parking')
        if parking:
            if "N" in parking:
                item_loader.add_value("parking", False)
            elif "Y" in parking:
                item_loader.add_value("parking", True)
        elevator = response.xpath("//div[@class='bullets-li']/p[contains(.,'Lift')]//text()").extract_first()
        if elevator:
            item_loader.add_value("elevator",True)
        floor = response.xpath("//div[@class='bullets-li']/p[contains(.,'FLOOR') and not(contains(.,'FLOORING'))]//text()").extract_first()
        if floor:
            item_loader.add_value("floor",floor.split("FLOOR")[0].strip())
        title =" ".join(response.xpath("//h1/a//text()").extract())
        if title:
            item_loader.add_value("title", title.strip())
        
        address = " ".join(response.xpath("//h1/a/text()").extract())
        if address:
            item_loader.add_value("address", address.replace("- ","").strip())
            item_loader.add_value("city", address.split(",")[-2].strip())
            item_loader.add_value("zipcode", address.split(",")[-1].strip())
        rent = " ".join(response.xpath("//span[@class='priceask']//text()").extract())
        if rent:
            if " pw" in rent.lower():
                rent = rent.split('£')[-1].split(' pw')[0].strip().replace(',', '').replace('\xa0', '')
                item_loader.add_value("rent", str(int(float(rent)) * 4))    
                item_loader.add_value("currency", 'GBP')            
            else:
                item_loader.add_value("rent_string",rent)
       
        images = [x for x in response.xpath("//div[@class='sp-thumbnails']/div//img/@src").extract()]
        if images:
                item_loader.add_value("images", images)
      
        desc = " ".join(response.xpath("//div[@class='details-information']/p//text()").extract())
        if desc:
            item_loader.add_value("description", desc.strip())
        
        latitude = response.xpath("//script[contains(.,'latitude') and contains(.,'map')]/text()").get()
        if latitude:
            item_loader.add_value("latitude", latitude.split('latitude":"')[1].split('"')[0].strip())
            item_loader.add_value("longitude", latitude.split('longitude":"')[1].split('"')[0].strip())

        item_loader.add_value("external_id", response.url.split('/')[-2].split('and-')[-1])

        item_loader.add_value("landlord_name", "Andersons Estates")
        item_loader.add_value("landlord_phone", "020 8830 4114")
        item_loader.add_value("landlord_email", "info@andersonsestates.co.uk")  

        yield item_loader.load_item()
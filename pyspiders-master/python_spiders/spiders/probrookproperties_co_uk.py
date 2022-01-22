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
    name = 'probrookproperties_co_uk'     
    execution_type = "testing"
    country = "united_kingdom"
    locale = "en"
    thousand_separator = ','
    scale_separator = '.'  
    def start_requests(self):

        url="https://probrookproperties.co.uk/properties/lettings/"
        yield Request(url,callback=self.parse)
    # 1. FOLLOWING 
    def parse(self, response):
        page = response.meta.get("page", 2)
        seen=False
        for item in response.xpath("//div[@class='col-xl-4 col-md-6 col']//a/@href").getall():
            follow_url = response.urljoin(item)
            seen=True
            yield Request(follow_url, callback=self.populate_item,meta={"property_type":"apartment"})
        if page == 2 or seen:
            url = f"https://probrookproperties.co.uk/properties/lettings/?pg=2&drawMap="
            yield Request(url, callback=self.parse, meta={"page": page+1,"property_type":"apartment"})

    # 2. SCRAPING level 2
    def populate_item(self, response):

        item_loader = ListingLoader(response=response) 

        item_loader.add_value("external_link", response.url)
        item_loader.add_value("property_type", response.meta["property_type"])
        item_loader.add_value("external_source", "Probrookproperties_Co_PySpider_united_kingdom")
        title="".join(response.xpath("//h1/span/text()").getall())
        if title:
            title=title.replace("\n","").replace("\t","").strip() 
            item_loader.add_value("title",title)   
        address ="".join(response.xpath("//h1/span/text()").getall())
        if address:
            address=address.replace("\n","").replace("\t","").strip() 
            item_loader.add_value("address",address.strip())
            city=address.split(",")[-2]
            item_loader.add_value("city",city)
            zipcode = address.split(",")[-1].strip()
            item_loader.add_value("zipcode",zipcode)

        rent = response.xpath("//div[@class='price']//span[@class='amount']/text()").get()
        if rent:
            item_loader.add_value("rent_string", rent)   
        deposit =response.xpath("//span[contains(.,'Deposit:')]/following-sibling::text()").get()
        if deposit:
            deposit=deposit.replace("£","").strip()
            item_loader.add_value("deposit", deposit)
        parking =response.xpath("//span[contains(.,'Parking:')]/following-sibling::text()").get()
        if parking:
            item_loader.add_value("parking", True) 
   
        room_count =response.xpath("//span[@class='pm-wrapper pm-bedrooms']/span[@class='counter']/text()").get()
        if room_count:   
            item_loader.add_value("room_count",room_count)
        bathroom_count =response.xpath("//span[@class='pm-wrapper pm-bathrooms']/span[@class='counter']/text()").get()
        if bathroom_count:   
            item_loader.add_value("bathroom_count",bathroom_count)
     
        images =[ x for x in response.xpath("//section[@id='property-gallery']/div[@id='slider-for']//img/@src").extract()]
        if images:
            item_loader.add_value("images", images) 

        lat=response.xpath("//div[@id='property-map']/@data-lat").get()
        if lat:
            item_loader.add_value("latitude", lat)
        lng=response.xpath("//div[@id='property-map']/@data-lng").get()
        if lng:
            item_loader.add_value("longitude", lng)
       
        desc = response.xpath("//div[@class='accordion-content']/p/text()").get()
        if desc:
            item_loader.add_value("description", desc.strip())
 
        item_loader.add_value("landlord_name", "ProBrook Properties")
        item_loader.add_value("landlord_phone", "0141 339 3050")
        item_loader.add_value("landlord_email", "david@probrookproperties.co.uk")  
    
        yield item_loader.load_item()
